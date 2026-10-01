package storage

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"example.com/spillway-gate-coordinator/internal/domain"
)

var commandSerial int64

func newTestGate(t *testing.T, repo *MemoryRepository, id string) {
	t.Helper()
	gate, err := domain.NewGate(id, id+" name", 5, 0.5, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("new gate: %v", err)
	}
	if err := repo.CreateGate(context.Background(), gate); err != nil {
		t.Fatalf("create gate: %v", err)
	}
}

func newTestCommand(t *testing.T, repo *MemoryRepository, gateID string, target float64) domain.GateCommand {
	t.Helper()
	gate, err := repo.Gate(context.Background(), gateID)
	if err != nil {
		t.Fatalf("load gate: %v", err)
	}
	serial := atomic.AddInt64(&commandSerial, 1)
	command, err := domain.NewGateCommand(
		"cmd-"+gateID+"-"+strconv.FormatInt(serial, 10),
		gateID,
		target,
		gate.Aperture,
		gate.MaxAperture,
		"test reason",
		"dispatcher-test",
		time.Unix(2, 0),
	)
	if err != nil {
		t.Fatalf("new command: %v", err)
	}
	if err := repo.CreateCommand(context.Background(), command); err != nil {
		t.Fatalf("create command: %v", err)
	}
	return command
}

func clearDecision(t *testing.T) domain.InterlockDecision {
	t.Helper()
	decision, err := domain.NewInterlockDecision("clear", "safety-tester", "conditions clear", time.Unix(3, 0))
	if err != nil {
		t.Fatalf("decision: %v", err)
	}
	return decision
}

// Conflicting clear reviews must leave the losing command untouched:
// the conflict response must not be followed by an approved review record.
func TestReviewConflictDoesNotPolluteCommand(t *testing.T) {
	repo := NewMemoryRepository()
	newTestGate(t, repo, "gate-a")
	first := newTestCommand(t, repo, "gate-a", 1.5)
	second := newTestCommand(t, repo, "gate-a", 2.5)

	if _, _, err := repo.ReviewInterlock(context.Background(), first.ID, clearDecision(t), "token-first", time.Unix(4, 0)); err != nil {
		t.Fatalf("first review: %v", err)
	}

	if _, _, err := repo.ReviewInterlock(context.Background(), second.ID, clearDecision(t), "token-second", time.Unix(5, 0)); err == nil {
		t.Fatalf("second review on reserved gate must conflict")
	}

	storedSecond, err := repo.Command(context.Background(), second.ID)
	if err != nil {
		t.Fatalf("load second command: %v", err)
	}
	if storedSecond.Status != domain.CommandPending {
		t.Fatalf("losing command mutated: status=%q, want pending", storedSecond.Status)
	}
	if storedSecond.Interlock != nil {
		t.Fatalf("losing command carries a review decision despite the conflict")
	}
	if storedSecond.ExecutionToken != "" {
		t.Fatalf("losing command carries an execution token despite the conflict")
	}

	gate, _ := repo.Gate(context.Background(), "gate-a")
	if gate.Status != domain.GateReserved || gate.ActiveCommandID != first.ID {
		t.Fatalf("gate reservation altered by losing review: status=%q active=%q", gate.Status, gate.ActiveCommandID)
	}

	// After the first command finishes and releases the gate, the still-pending
	// second application must remain available for another dispatcher.
	approvedFirst, err := repo.Command(context.Background(), first.ID)
	if err != nil {
		t.Fatalf("load first command: %v", err)
	}
	if _, _, err := repo.ExecuteCommand(context.Background(), first.ID, approvedFirst.ExecutionToken, "operator-tester", false, time.Unix(6, 0)); err != nil {
		t.Fatalf("execute first command: %v", err)
	}
	if _, gateAfter, err := repo.ReviewInterlock(context.Background(), second.ID, clearDecision(t), "token-second-retry", time.Unix(7, 0)); err != nil {
		t.Fatalf("pending second command should be reviewable after release: %v", err)
	} else if gateAfter.ActiveCommandID != second.ID {
		t.Fatalf("second review reserved %q, want %q", gateAfter.ActiveCommandID, second.ID)
	}
}

// Concurrent clear reviews on the same gate must produce exactly one winner;
// the gate reservation can never be stolen by a losing review.
func TestConcurrentClearReviewsHaveSingleWinner(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		repo := NewMemoryRepository()
		newTestGate(t, repo, "gate-c")
		first := newTestCommand(t, repo, "gate-c", 1.5)
		second := newTestCommand(t, repo, "gate-c", 2.5)

		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() {
			<-start
			_, _, err := repo.ReviewInterlock(context.Background(), first.ID, clearDecision(t), "token-first", time.Unix(4, 0))
			errs <- err
		}()
		go func() {
			<-start
			_, _, err := repo.ReviewInterlock(context.Background(), second.ID, clearDecision(t), "token-second", time.Unix(5, 0))
			errs <- err
		}()
		close(start)

		conflicts := 0
		for i := 0; i < 2; i++ {
			if err := <-errs; err != nil {
				if domain.ErrorCodeOf(err) == "gate_not_idle" || domain.ErrorCodeOf(err) == "gate_already_reserved" {
					conflicts++
					continue
				}
				t.Fatalf("unexpected review error: %v", err)
			}
		}
		if conflicts != 1 {
			t.Fatalf("attempt %d: expected exactly one conflict, got %d", attempt, conflicts)
		}

		gate, _ := repo.Gate(context.Background(), "gate-c")
		if gate.Status != domain.GateReserved {
			t.Fatalf("attempt %d: gate status=%q, want reserved", attempt, gate.Status)
		}
		storedFirst, _ := repo.Command(context.Background(), first.ID)
		storedSecond, _ := repo.Command(context.Background(), second.ID)
		approved := 0
		if storedFirst.Status == domain.CommandApproved {
			approved++
		}
		if storedSecond.Status == domain.CommandApproved {
			approved++
		}
		if approved != 1 {
			t.Fatalf("attempt %d: approved commands=%d, want exactly 1", attempt, approved)
		}
		winner := first.ID
		if storedSecond.Status == domain.CommandApproved {
			winner = second.ID
		}
		if gate.ActiveCommandID != winner {
			t.Fatalf("attempt %d: gate reserved by %q, approved command is %q", attempt, gate.ActiveCommandID, winner)
		}
	}
}

// A simulated fault must atomically abort the command, clear the token, and
// latch the gate so the execution visibly ends and cannot be driven again.
func TestFaultExecutionCommitsAbortedCommand(t *testing.T) {
	repo := NewMemoryRepository()
	newTestGate(t, repo, "gate-f")
	command := newTestCommand(t, repo, "gate-f", 1.5)

	approved, _, err := repo.ReviewInterlock(context.Background(), command.ID, clearDecision(t), "token-fault", time.Unix(4, 0))
	if err != nil {
		t.Fatalf("review: %v", err)
	}
	if approved.ExecutionToken != "token-fault" {
		t.Fatalf("unexpected token %q", approved.ExecutionToken)
	}

	finished, gate, err := repo.ExecuteCommand(context.Background(), command.ID, "token-fault", "operator-tester", true, time.Unix(5, 0))
	if err != nil {
		t.Fatalf("fault execution: %v", err)
	}
	if finished.Status != domain.CommandAborted {
		t.Fatalf("returned command status=%q, want aborted", finished.Status)
	}
	if finished.ExecutionToken != "" {
		t.Fatalf("token not cleared on aborted command: %q", finished.ExecutionToken)
	}
	if gate.Status != domain.GateFaulted || gate.ActiveCommandID != "" || gate.Aperture != 1.5 {
		t.Fatalf("gate not latched: status=%q active=%q aperture=%v", gate.Status, gate.ActiveCommandID, gate.Aperture)
	}

	stored, err := repo.Command(context.Background(), command.ID)
	if err != nil {
		t.Fatalf("reload command: %v", err)
	}
	if stored.Status != domain.CommandAborted || stored.ExecutionToken != "" {
		t.Fatalf("persisted command status=%q token=%q, want aborted with empty token", stored.Status, stored.ExecutionToken)
	}

	// The consumed credential must not drive the now-terminal command again.
	if _, _, err := repo.ExecuteCommand(context.Background(), command.ID, "token-fault", "operator-tester", true, time.Unix(6, 0)); err == nil {
		t.Fatalf("replaying the fault execution must fail")
	}
}

// Concurrent execution attempts with the same token must succeed exactly once.
func TestConcurrentExecutionTokenSucceedsOnce(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		repo := NewMemoryRepository()
		newTestGate(t, repo, "gate-e")
		command := newTestCommand(t, repo, "gate-e", 1.5)
		if _, _, err := repo.ReviewInterlock(context.Background(), command.ID, clearDecision(t), "token-exec", time.Unix(4, 0)); err != nil {
			t.Fatalf("review: %v", err)
		}

		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() {
			<-start
			_, _, err := repo.ExecuteCommand(context.Background(), command.ID, "token-exec", "operator-a", false, time.Unix(5, 0))
			errs <- err
		}()
		go func() {
			<-start
			_, _, err := repo.ExecuteCommand(context.Background(), command.ID, "token-exec", "operator-b", false, time.Unix(5, 0))
			errs <- err
		}()
		close(start)

		successes, conflicts := 0, 0
		for i := 0; i < 2; i++ {
			switch err := <-errs; {
			case err == nil:
				successes++
			case domain.ErrorKindOf(err) == domain.KindConflict:
				conflicts++
			default:
				t.Fatalf("unexpected execution error: %v", err)
			}
		}
		if successes != 1 || conflicts != 1 {
			t.Fatalf("attempt %d: successes=%d conflicts=%d, want 1 and 1", attempt, successes, conflicts)
		}
		stored, _ := repo.Command(context.Background(), command.ID)
		if stored.Status != domain.CommandCompleted || stored.ExecutionToken != "" {
			t.Fatalf("attempt %d: command status=%q token=%q, want completed with empty token", attempt, stored.Status, stored.ExecutionToken)
		}
	}
}

// Concurrent fault submissions with the same credential must latch the fault
// exactly once; the loser must not leave an executing command behind.
func TestConcurrentFaultSubmissionLatchesOnce(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		repo := NewMemoryRepository()
		newTestGate(t, repo, "gate-g")
		command := newTestCommand(t, repo, "gate-g", 1.5)
		if _, _, err := repo.ReviewInterlock(context.Background(), command.ID, clearDecision(t), "token-faultx", time.Unix(4, 0)); err != nil {
			t.Fatalf("review: %v", err)
		}

		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() {
			<-start
			_, _, err := repo.ExecuteCommand(context.Background(), command.ID, "token-faultx", "operator-a", true, time.Unix(5, 0))
			errs <- err
		}()
		go func() {
			<-start
			_, _, err := repo.ExecuteCommand(context.Background(), command.ID, "token-faultx", "operator-b", true, time.Unix(5, 0))
			errs <- err
		}()
		close(start)

		successes, conflicts := 0, 0
		for i := 0; i < 2; i++ {
			switch err := <-errs; {
			case err == nil:
				successes++
			case domain.ErrorKindOf(err) == domain.KindConflict:
				conflicts++
			default:
				t.Fatalf("unexpected fault execution error: %v", err)
			}
		}
		if successes != 1 || conflicts != 1 {
			t.Fatalf("attempt %d: successes=%d conflicts=%d, want 1 and 1", attempt, successes, conflicts)
		}
		stored, _ := repo.Command(context.Background(), command.ID)
		if stored.Status != domain.CommandAborted || stored.ExecutionToken != "" {
			t.Fatalf("attempt %d: command status=%q token=%q, want aborted with empty token", attempt, stored.Status, stored.ExecutionToken)
		}
		gate, _ := repo.Gate(context.Background(), "gate-g")
		if gate.Status != domain.GateFaulted || gate.ActiveCommandID != "" {
			t.Fatalf("attempt %d: gate status=%q active=%q, want faulted and released", attempt, gate.Status, gate.ActiveCommandID)
		}
	}
}

// A blocked review on another pending command must not touch the gate
// reservation owned by an already approved command.
func TestBlockedReviewLeavesReservationUntouched(t *testing.T) {
	repo := NewMemoryRepository()
	newTestGate(t, repo, "gate-b")
	first := newTestCommand(t, repo, "gate-b", 1.5)
	second := newTestCommand(t, repo, "gate-b", 2.5)

	if _, _, err := repo.ReviewInterlock(context.Background(), first.ID, clearDecision(t), "token-b", time.Unix(4, 0)); err != nil {
		t.Fatalf("first review: %v", err)
	}
	blocked, err := domain.NewInterlockDecision("blocked", "safety-tester", "conditions unsafe", time.Unix(5, 0))
	if err != nil {
		t.Fatalf("blocked decision: %v", err)
	}
	finished, gate, err := repo.ReviewInterlock(context.Background(), second.ID, blocked, "", time.Unix(5, 0))
	if err != nil {
		t.Fatalf("blocked review on pending command should succeed: %v", err)
	}
	if finished.Status != domain.CommandRejected || finished.ExecutionToken != "" {
		t.Fatalf("blocked command status=%q token=%q, want rejected without token", finished.Status, finished.ExecutionToken)
	}
	if gate.Status != domain.GateReserved || gate.ActiveCommandID != first.ID {
		t.Fatalf("blocked review disturbed reservation: status=%q active=%q", gate.Status, gate.ActiveCommandID)
	}
}
