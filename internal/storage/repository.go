package storage

import (
	"context"
	"time"

	"example.com/spillway-gate-coordinator/internal/domain"
)

type ReviewCommit struct {
	Command    domain.GateCommand
	Gate       domain.Gate
	CommitGate bool
	Conflict   error
}

type ExecutionCommit struct {
	RunningCommand domain.GateCommand
	RunningGate    domain.Gate
	FinalCommand   domain.GateCommand
	FinalGate      domain.Gate
	Faulted        bool
}

type Repository interface {
	CreateGate(ctx context.Context, gate domain.Gate) error
	Gate(ctx context.Context, id string) (domain.Gate, error)
	CreateCommand(ctx context.Context, command domain.GateCommand) error
	Command(ctx context.Context, id string) (domain.GateCommand, error)
	PlanReview(
		ctx context.Context,
		commandID string,
		decision domain.InterlockDecision,
		token string,
		now time.Time,
	) (ReviewCommit, error)
	ApplyReview(
		ctx context.Context,
		commit ReviewCommit,
	) (domain.GateCommand, domain.Gate, error)
	PlanExecution(
		ctx context.Context,
		commandID string,
		token string,
		operator string,
		simulateFault bool,
		now time.Time,
	) (ExecutionCommit, error)
	ApplyExecution(
		ctx context.Context,
		commit ExecutionCommit,
	) (domain.GateCommand, domain.Gate, error)
}
