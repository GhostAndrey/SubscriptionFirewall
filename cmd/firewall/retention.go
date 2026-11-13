package main

import (
	"context"
	"log/slog"
	"time"

	"subscriptionfirewall/internal/ports"
)

type historyPruner struct {
	transactions         ports.TransactionRepository
	audit                ports.AuditLog
	transactionRetention time.Duration
	auditRetention       time.Duration
	logger               *slog.Logger
}

func (p *historyPruner) prune(ctx context.Context) {
	if p == nil {
		return
	}
	p.pruneTransactions(ctx)
	p.pruneAudit(ctx)
}

func (p *historyPruner) pruneTransactions(ctx context.Context) {
	if p.transactionRetention <= 0 || p.transactions == nil {
		return
	}
	deleted, err := p.transactions.DeleteOlderThan(ctx, p.transactionRetention)
	if err != nil {
		p.logger.Error("delete old transactions failed", "error", err)
	} else if deleted > 0 {
		p.logger.Info("deleted old transactions", "count", deleted)
	}
}

func (p *historyPruner) pruneAudit(ctx context.Context) {
	if p.auditRetention <= 0 || p.audit == nil {
		return
	}
	deleted, err := p.audit.DeleteOlderThan(ctx, p.auditRetention)
	if err != nil {
		p.logger.Error("delete old audit entries failed", "error", err)
	} else if deleted > 0 {
		p.logger.Info("deleted old audit entries", "count", deleted)
	}
}
