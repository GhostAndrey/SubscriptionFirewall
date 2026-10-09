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
	if !action.isValid() {
		return nil, fmt.Errorf("unknown lifecycle action %q: %w", action, domain.ErrInvalidTransition)
	}

	changed, err := s.applySubscription(ctx, id, action)
	if err != nil {
		return nil, err
	}
	if err := s.record(ctx, actor, action, entitySubscription, string(id)); err != nil {
		s.logger.Warn("audit record failed", "action", action, "entity_type", entitySubscription, "entity_id", string(id), "error", err)
	}

	if changed.VirtualTokenID == "" {
		s.count(action, entitySubscription, "applied")
		return changed, nil
	}
	tokenID := domain.VirtualTokenID(changed.VirtualTokenID)

	if _, err := s.applyToken(ctx, tokenID, action); err != nil {
		return changed, s.handleCounterpartFailure(ctx, entitySubscription, changed, entityToken, string(tokenID), action, err)
	}
	if err := s.record(ctx, actor, action, entityToken, string(tokenID)); err != nil {
		s.logger.Warn("audit record failed", "action", action, "entity_type", entityToken, "entity_id", string(tokenID), "error", err)
	}
	s.count(action, entityToken, "applied")
	s.count(action, entitySubscription, "applied")
	return changed, nil
}

func (s *Service) ChangeToken(ctx context.Context, actor string, id domain.VirtualTokenID, action Action) (*domain.VirtualToken, error) {
	if !action.isValid() {
		return nil, fmt.Errorf("unknown lifecycle action %q: %w", action, domain.ErrInvalidTransition)
	}

	changed, err := s.applyToken(ctx, id, action)
	if err != nil {
		return nil, err
	}
	if err := s.record(ctx, actor, action, entityToken, string(id)); err != nil {
		s.logger.Warn("audit record failed", "action", action, "entity_type", entityToken, "entity_id", string(id), "error", err)
	}

	linked, err := s.subscriptions.GetByUserAndMerchant(ctx, changed.UserID, changed.MerchantID)
	if errors.Is(err, domain.ErrNotFound) {
		s.count(action, entityToken, "applied")
		return changed, nil
	}
	if err != nil {
		return changed, s.handleCounterpartFailure(ctx, entityToken, changed, entitySubscription, string(linked.ID), action, err)
	}

	if _, err := s.applySubscription(ctx, linked.ID, action); err != nil {
		return changed, s.handleCounterpartFailure(ctx, entityToken, changed, entitySubscription, string(linked.ID), action, err)
	}
	if err := s.record(ctx, actor, action, entitySubscription, string(linked.ID)); err != nil {
		s.logger.Warn("audit record failed", "action", action, "entity_type", entitySubscription, "entity_id", string(linked.ID), "error", err)
	}
	s.count(action, entitySubscription, "applied")
	s.count(action, entityToken, "applied")
	return changed, nil
}

// SyncZombie freezes the card of a subscription that stopped charging, so a
// zombie subscription can no longer authorize merchant charges. The
// subscription transition belongs to the sweep and is never compensated here:
// a zombie must stay a zombie, so the card freeze is only ever deferred.
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

	if err := s.record(ctx, actorSystem, ActionFreeze, entityToken, string(tokenID)); err != nil {
		s.logger.Warn("audit record failed", "action", ActionFreeze, "entity_type", entityToken, "entity_id", string(tokenID), "error", err)
	}
	s.count(ActionFreeze, entityToken, "applied")
	return nil
}

const actorSystem = "system"

// handleCounterpartFailure either compensates the change already applied or,
// when the action cannot be undone, defers the counterpart to the sync queue.
// It always returns a non-nil error so the caller cannot mistake a partial
// transition for a completed one.
func (s *Service) handleCounterpartFailure(
	ctx context.Context,
	originEntity string,
	origin any,
	targetEntity string,
	counterpartID string,
	action Action,
	cause error,
) error {
	s.countLinkedFailure(targetEntity, string(action))
	s.count(action, originEntity, "partial")

	if _, reversible := action.compensate(); reversible {
		if err := s.compensate(ctx, action, origin); err != nil {
			s.logger.Error("compensating lifecycle action failed",
				"action", string(action), "entity_type", originEntity, "error", err)
			return errors.Join(cause, err)
		}
		s.count(action, originEntity, "compensated")
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

// compensate undoes a reversible action on the entity it was already applied
// to, so that a failed counterpart update never leaves a half-applied pair.
func (s *Service) compensate(ctx context.Context, action Action, origin any) error {
	inverse, ok := action.compensate()
	if !ok {
		return nil
	}
	switch value := origin.(type) {
	case *domain.Subscription:
		_, err := s.applySubscription(ctx, value.ID, inverse)
		return err
	case *domain.VirtualToken:
		_, err := s.applyToken(ctx, value.ID, inverse)
		return err
	default:
		return fmt.Errorf("cannot compensate entity of type %T", origin)
	}
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
		_, err := s.applyToken(ctx, domain.VirtualTokenID(task.EntityID), action)
		return err
	case entitySubscription:
		_, err := s.applySubscription(ctx, domain.SubscriptionID(task.EntityID), action)
		return err
	default:
		return fmt.Errorf("unknown lifecycle entity type %q", task.EntityType)
	}
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
