package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

type Action string

const (
	ActionFreeze     Action = "freeze"
	ActionReactivate Action = "reactivate"
	ActionTerminate  Action = "terminate"
)

func (a Action) isValid() bool {
	switch a {
	case ActionFreeze, ActionReactivate, ActionTerminate:
		return true
	default:
		return false
	}
}

func (a Action) compensate() (Action, bool) {
	switch a {
	case ActionFreeze:
		return ActionReactivate, true
	case ActionReactivate:
		return ActionFreeze, true
	default:
		return "", false
	}
}

const (
	entitySubscription = "subscription"
	entityToken        = "token"
)

// ErrSyncDeferred reports that an irreversible action reached only its own
// entity; the counterpart is queued for a later attempt.
var ErrSyncDeferred = errors.New("counterpart lifecycle action deferred")

// Service keeps a subscription and its virtual card in the same lifecycle
// state. Every transition is applied through this service so the pairing can
// never be skipped by a new call site.
type Service struct {
	subscriptions *subscription.Service
	tokens        *token.Service
	queue         ports.LifecycleSyncQueue
	audit         ports.AuditLog
	metrics       *obs.Metrics
	logger        *slog.Logger
}

type Options struct {
	Queue   ports.LifecycleSyncQueue
	Audit   ports.AuditLog
	Metrics *obs.Metrics
	Logger  *slog.Logger
}

func NewService(subscriptions *subscription.Service, tokens *token.Service, options Options) *Service {
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		subscriptions: subscriptions,
		tokens:        tokens,
		queue:         options.Queue,
		audit:         options.Audit,
		metrics:       options.Metrics,
		logger:        logger,
	}
}

func (s *Service) FreezeSubscription(ctx context.Context, actor string, id domain.SubscriptionID) (*domain.Subscription, error) {
	return s.ChangeSubscription(ctx, actor, id, ActionFreeze)
}

func (s *Service) ReactivateSubscription(ctx context.Context, actor string, id domain.SubscriptionID) (*domain.Subscription, error) {
	return s.ChangeSubscription(ctx, actor, id, ActionReactivate)
}

func (s *Service) TerminateSubscription(ctx context.Context, actor string, id domain.SubscriptionID) (*domain.Subscription, error) {
	return s.ChangeSubscription(ctx, actor, id, ActionTerminate)
}

func (s *Service) FreezeToken(ctx context.Context, actor string, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	return s.ChangeToken(ctx, actor, id, ActionFreeze)
}

func (s *Service) ReactivateToken(ctx context.Context, actor string, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	return s.ChangeToken(ctx, actor, id, ActionReactivate)
}

func (s *Service) TerminateToken(ctx context.Context, actor string, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	return s.ChangeToken(ctx, actor, id, ActionTerminate)
}

func (s *Service) ChangeSubscription(ctx context.Context, actor string, id domain.SubscriptionID, action Action) (*domain.Subscription, error) {
	if err := validateAction(action); err != nil {
		return nil, err
	}

	changed, err := s.applySubscription(ctx, id, action)
	if err != nil {
		return nil, err
	}

	if changed.VirtualTokenID == "" {
		s.recordAppliedOrigin(ctx, actor, action, entitySubscription, string(id))
		return changed, nil
	}
	tokenID := domain.VirtualTokenID(changed.VirtualTokenID)
	if _, err := s.applyToken(ctx, tokenID, action); err != nil {
		return changed, s.finishCounterpartFailure(ctx, actor, action, changeOf(changed), entityToken, string(tokenID), err)
	}
	s.recordAppliedOrigin(ctx, actor, action, entitySubscription, string(id))
	s.writeAudit(ctx, actor, action, entityToken, string(tokenID))
	s.count(action, entityToken, "applied")
	return changed, nil
}

func (s *Service) ChangeToken(ctx context.Context, actor string, id domain.VirtualTokenID, action Action) (*domain.VirtualToken, error) {
	if err := validateAction(action); err != nil {
		return nil, err
	}

	changed, err := s.applyToken(ctx, id, action)
	if err != nil {
		return nil, err
	}

	linked, err := s.subscriptions.GetByUserAndMerchant(ctx, changed.UserID, changed.MerchantID)
	switch {
	case errors.Is(err, domain.ErrNotFound):
		s.recordAppliedOrigin(ctx, actor, action, entityToken, string(id))
		return changed, nil
	case err != nil:
		return changed, s.finishCounterpartFailure(ctx, actor, action, changeOfToken(changed), entitySubscription, "", err)
	}

	if _, err := s.applySubscription(ctx, linked.ID, action); err != nil {
		return changed, s.finishCounterpartFailure(ctx, actor, action, changeOfToken(changed), entitySubscription, string(linked.ID), err)
	}
	s.recordAppliedOrigin(ctx, actor, action, entityToken, string(id))
	s.writeAudit(ctx, actor, action, entitySubscription, string(linked.ID))
	s.count(action, entitySubscription, "applied")
	return changed, nil
}

// SyncZombie freezes the card of a subscription that stopped charging. A
// zombie must stay a zombie, so the card freeze is never compensated, only
// deferred.
func (s *Service) SyncZombie(ctx context.Context, subscription *domain.Subscription) error {
	if subscription.State != domain.SubscriptionZombie || subscription.VirtualTokenID == "" {
		return nil
	}
	tokenID := domain.VirtualTokenID(subscription.VirtualTokenID)

	_, err := s.applyToken(ctx, tokenID, ActionFreeze)
	switch {
	case err == nil:
	case errors.Is(err, domain.ErrNotFound):
		s.logger.Info("zombie subscription has no card to freeze",
			"subscription_id", string(subscription.ID), "virtual_token_id", string(tokenID))
		return nil
	default:
		return s.deferCounterpart(ctx, ActionFreeze, entityToken, string(tokenID), err)
	}

	s.writeAudit(ctx, actorSystem, ActionFreeze, entityToken, string(tokenID))
	s.count(ActionFreeze, entityToken, "applied")
	return nil
}

const actorSystem = "system"

func validateAction(action Action) error {
	if !action.isValid() {
		return fmt.Errorf("unknown lifecycle action %q: %w", action, domain.ErrInvalidTransition)
	}
	return nil
}

// writeAudit records an entry, downgrading a storage failure to a warning: losing
// the audit trail must not undo a transition that already reached both
// entities.
func (s *Service) writeAudit(ctx context.Context, actor string, action Action, entityType, entityID string) {
	if err := s.record(ctx, actor, action, entityType, entityID); err != nil {
		s.logger.Warn("audit record failed",
			"action", action, "entity_type", entityType, "entity_id", entityID, "error", err)
	}
}

// recordAppliedOrigin audits an origin change once it is final, and counts it as
// applied. Callers use it after the counterpart either succeeded or was
// deferred, so a compensated (rolled back) change is never audited.
func (s *Service) recordAppliedOrigin(ctx context.Context, actor string, action Action, entityType, entityID string) {
	s.writeAudit(ctx, actor, action, entityType, entityID)
	s.count(action, entityType, "applied")
}

// finishCounterpartFailure resolves a failed counterpart, then audits the
// origin only when the change stands: a deferred irreversible action is final,
// while a compensated reversible one was rolled back and must not be recorded.
func (s *Service) finishCounterpartFailure(
	ctx context.Context,
	actor string,
	action Action,
	origin changeable,
	targetEntity string,
	counterpartID string,
	cause error,
) error {
	err := s.handleCounterpartFailure(ctx, origin, targetEntity, counterpartID, action, cause)
	if errors.Is(err, ErrSyncDeferred) {
		s.recordAppliedOrigin(ctx, actor, action, origin.entityType(), origin.entityID())
	}
	return err
}

type changeable interface {
	entityType() string
	entityID() string
	apply(ctx context.Context, s *Service, action Action) error
}

func changeOf(subscription *domain.Subscription) changeable {
	return subscriptionChange{subscription.ID}
}
func changeOfToken(token *domain.VirtualToken) changeable { return tokenChange{token.ID} }

type subscriptionChange struct{ id domain.SubscriptionID }

func (subscriptionChange) entityType() string { return entitySubscription }
func (c subscriptionChange) entityID() string { return string(c.id) }
func (c subscriptionChange) apply(ctx context.Context, s *Service, action Action) error {
	_, err := s.applySubscription(ctx, c.id, action)
	return err
}

type tokenChange struct{ id domain.VirtualTokenID }

func (tokenChange) entityType() string { return entityToken }
func (c tokenChange) entityID() string { return string(c.id) }
func (c tokenChange) apply(ctx context.Context, s *Service, action Action) error {
	_, err := s.applyToken(ctx, c.id, action)
	return err
}

// handleCounterpartFailure either compensates the change already applied or,
// when the action cannot be undone, defers the counterpart to the sync queue.
// It always returns a non-nil error so the caller cannot mistake a partial
// transition for a completed one.
func (s *Service) handleCounterpartFailure(
	ctx context.Context,
	origin changeable,
	targetEntity string,
	counterpartID string,
	action Action,
	cause error,
) error {
	s.countLinkedFailure(targetEntity, string(action))
	s.count(action, origin.entityType(), "partial")

	if inverse, reversible := action.compensate(); reversible {
		if err := origin.apply(ctx, s, inverse); err != nil {
			s.logger.Error("compensating lifecycle action failed",
				"action", string(action), "entity_type", origin.entityType(), "error", err)
			return errors.Join(cause, err)
		}
		s.count(action, origin.entityType(), "compensated")
		return fmt.Errorf("counterpart %s %s not applied, change rolled back: %w", targetEntity, counterpartID, cause)
	}
	return s.deferCounterpart(ctx, action, targetEntity, counterpartID, cause)
}

// deferCounterpart stores an action for the syncer so it eventually reaches the
// counterpart entity after a crash, a restart or a transient failure.
func (s *Service) deferCounterpart(ctx context.Context, action Action, targetEntity, counterpartID string, cause error) error {
	if s.queue == nil {
		return fmt.Errorf("counterpart %s %s not applied and no sync queue configured: %w", targetEntity, counterpartID, cause)
	}
	task := ports.LifecycleSyncTask{
		EntityType: targetEntity,
		EntityID:   counterpartID,
		Action:     string(action),
	}
	if err := s.queue.Enqueue(ctx, task); err != nil {
		s.logger.Error("enqueue lifecycle sync failed",
			"action", string(action), "entity_type", targetEntity, "entity_id", task.EntityID, "error", err)
		return errors.Join(cause, err)
	}
	s.count(action, targetEntity, "deferred")
	s.logger.Warn("counterpart lifecycle action deferred",
		"action", string(action), "entity_type", targetEntity,
		"entity_id", task.EntityID, "error", cause,
	)
	return fmt.Errorf("%w: %s %s: %w", ErrSyncDeferred, targetEntity, task.EntityID, cause)
}

// ReplayTask applies a task that was deferred earlier. It is idempotent: an
// action already satisfied by the current state succeeds.
func (s *Service) ReplayTask(ctx context.Context, task ports.LifecycleSyncTask) error {
	action := Action(task.Action)
	if !action.isValid() {
		return fmt.Errorf("unknown lifecycle action %q: %w", task.Action, domain.ErrInvalidTransition)
	}

	switch task.EntityType {
	case entityToken:
		if _, err := s.applyToken(ctx, domain.VirtualTokenID(task.EntityID), action); err != nil {
			return err
		}
	case entitySubscription:
		if _, err := s.applySubscription(ctx, domain.SubscriptionID(task.EntityID), action); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown lifecycle entity type %q", task.EntityType)
	}
	s.recordAppliedOrigin(ctx, actorSystem, action, task.EntityType, task.EntityID)
	return nil
}

func (s *Service) applySubscription(ctx context.Context, id domain.SubscriptionID, action Action) (*domain.Subscription, error) {
	switch action {
	case ActionFreeze:
		return s.subscriptions.Freeze(ctx, id)
	case ActionReactivate:
		return s.subscriptions.Reactivate(ctx, id)
	case ActionTerminate:
		return s.subscriptions.Terminate(ctx, id)
	default:
		return nil, fmt.Errorf("unknown lifecycle action %q", action)
	}
}

func (s *Service) applyToken(ctx context.Context, id domain.VirtualTokenID, action Action) (*domain.VirtualToken, error) {
	switch action {
	case ActionFreeze:
		return s.tokens.Freeze(ctx, id)
	case ActionReactivate:
		return s.tokens.Reactivate(ctx, id)
	case ActionTerminate:
		return s.tokens.Terminate(ctx, id)
	default:
		return nil, fmt.Errorf("unknown lifecycle action %q", action)
	}
}

func (s *Service) record(ctx context.Context, actor string, action Action, entityType, entityID string) error {
	if s.audit == nil {
		return nil
	}
	return s.audit.Record(ctx, ports.AuditEntry{
		Actor:      actor,
		Action:     string(action),
		EntityType: entityType,
		EntityID:   entityID,
	})
}

func (s *Service) count(action Action, entity, result string) {
	if s.metrics == nil {
		return
	}
	s.metrics.CountLifecycleSync(entity, string(action), result)
}

func (s *Service) countLinkedFailure(entity, action string) {
	if s.metrics == nil {
		return
	}
	s.metrics.CountLinkedActionFailure(entity, action)
}
