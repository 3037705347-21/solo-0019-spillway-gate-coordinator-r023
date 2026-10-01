package bootstrap_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"example.com/spillway-gate-coordinator/internal/bootstrap"
)

type apiClient struct {
	baseURL string
}

func (c *apiClient) do(t *testing.T, method, path string, payload any) (int, map[string]any) {
	t.Helper()
	var body bytes.Buffer
	if payload != nil {
		if err := json.NewEncoder(&body).Encode(payload); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	request, err := http.NewRequest(method, c.baseURL+path, &body)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	decoded := map[string]any{}
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode %s %s: %v", method, path, err)
	}
	return response.StatusCode, decoded
}

func mustField(t *testing.T, body map[string]any, field string) map[string]any {
	t.Helper()
	value, ok := body[field].(map[string]any)
	if !ok {
		t.Fatalf("response missing object field %q: %v", field, body)
	}
	return value
}

func mustString(t *testing.T, object map[string]any, field string) string {
	t.Helper()
	value, ok := object[field].(string)
	if !ok {
		t.Fatalf("object missing string field %q: %v", field, object)
	}
	return value
}

// End-to-end version of the safety-review concurrency scenario: two clear
// reviews on the same gate fire simultaneously; exactly one wins and the
// losing application stays pending and reviewable once the gate frees up.
func TestConcurrentReviewsStayConsistent(t *testing.T) {
	server := httptest.NewServer(bootstrap.NewHandler())
	defer server.Close()
	client := &apiClient{baseURL: server.URL}

	status, body := client.do(t, http.MethodPost, "/gates", map[string]any{
		"id":               "gate-concurrent-review",
		"name":             "并发复核闸门",
		"max_aperture":     5,
		"initial_aperture": 0.25,
	})
	if status != http.StatusCreated {
		t.Fatalf("register gate: %d %v", status, body)
	}

	createCommand := func(target float64) string {
		status, body := client.do(t, http.MethodPost, "/commands", map[string]any{
			"gate_id":         "gate-concurrent-review",
			"target_aperture": target,
			"reason":          "并发复核",
			"requested_by":    "dispatcher-" + strconv.FormatFloat(target, 'f', -1, 64),
		})
		if status != http.StatusCreated {
			t.Fatalf("create command: %d %v", status, body)
		}
		return mustString(t, mustField(t, body, "command"), "id")
	}
	firstID := createCommand(1.5)
	secondID := createCommand(2.5)

	review := func(id string) (int, map[string]any) {
		return client.do(t, http.MethodPost, "/commands/"+id+"/interlock", map[string]any{
			"verdict":  "clear",
			"verifier": "safety-concurrent",
			"note":     "并发占用验证",
		})
	}

	start := make(chan struct{})
	type outcome struct {
		status int
		body   map[string]any
	}
	outcomes := make([]outcome, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		outcomes[0].status, outcomes[0].body = review(firstID)
	}()
	go func() {
		defer wg.Done()
		<-start
		outcomes[1].status, outcomes[1].body = review(secondID)
	}()
	close(start)
	wg.Wait()

	approved, conflicts := 0, 0
	winnerIndex := -1
	for i, result := range outcomes {
		switch result.status {
		case http.StatusOK:
			if mustString(t, mustField(t, result.body, "command"), "status") != "approved" {
				t.Fatalf("successful review did not approve command: %v", result.body)
			}
			approved++
			winnerIndex = i
		case http.StatusConflict:
			conflicts++
		default:
			t.Fatalf("unexpected review status %d: %v", result.status, result.body)
		}
	}
	if approved != 1 || conflicts != 1 {
		t.Fatalf("reviews: approved=%d conflicts=%d, want 1 and 1", approved, conflicts)
	}

	// The losing review must not have written a decision or token.
	loserID := secondID
	if winnerIndex == 1 {
		loserID = firstID
	}
	status, loserBody := client.do(t, http.MethodGet, "/commands/"+loserID, nil)
	if status != http.StatusOK {
		t.Fatalf("read losing command: %d %v", status, loserBody)
	}
	loserCommand := mustField(t, loserBody, "command")
	if got := mustString(t, loserCommand, "status"); got != "pending" {
		t.Fatalf("losing command status=%q, want pending", got)
	}
	if _, hasToken := loserCommand["execution_token"]; hasToken {
		t.Fatalf("losing command carries an execution token: %v", loserCommand)
	}
	if _, hasReview := loserCommand["interlock"]; hasReview {
		t.Fatalf("losing command carries a review record despite the conflict: %v", loserCommand)
	}

	// The gate must point at the single winning command.
	winnerID := firstID
	if winnerIndex == 1 {
		winnerID = secondID
	}
	status, gateBody := client.do(t, http.MethodGet, "/gates/gate-concurrent-review", nil)
	if status != http.StatusOK {
		t.Fatalf("read gate: %d %v", status, gateBody)
	}
	gate := mustField(t, gateBody, "gate")
	if mustString(t, gate, "status") != "reserved" || mustString(t, gate, "active_command_id") != winnerID {
		t.Fatalf("gate reservation inconsistent: status=%q active=%q winner=%q",
			gate["status"], gate["active_command_id"], winnerID)
	}

	// After the winner finishes, the pending loser must be reviewable and win.
	winningOutcome := outcomes[0]
	if winningOutcome.status != http.StatusOK {
		winningOutcome = outcomes[1]
	}
	token := mustString(t, mustField(t, winningOutcome.body, "command"), "execution_token")
	status, execBody := client.do(t, http.MethodPost, "/commands/"+winnerID+"/execute", map[string]any{
		"operator":       "operator-http",
		"token":          token,
		"simulate_fault": false,
	})
	if status != http.StatusOK {
		t.Fatalf("execute winner: %d %v", status, execBody)
	}
	status, retry := client.do(t, http.MethodPost, "/commands/"+loserID+"/interlock", map[string]any{
		"verdict":  "clear",
		"verifier": "safety-retry",
		"note":     "闸门释放后继续处理",
	})
	if status != http.StatusOK {
		t.Fatalf("pending loser should be reviewable after release: %d %v", status, retry)
	}
}

// End-to-end fault handling: the operator gets a success response and the
// dispatch side must see an aborted command, a faulted gate, and no reusable
// credential in the same response and every later read.
func TestFaultExecutionIsConsistent(t *testing.T) {
	server := httptest.NewServer(bootstrap.NewHandler())
	defer server.Close()
	client := &apiClient{baseURL: server.URL}

	if status, body := client.do(t, http.MethodPost, "/gates", map[string]any{
		"id":               "gate-fault-http",
		"name":             "故障处置闸门",
		"max_aperture":     5,
		"initial_aperture": 0.25,
	}); status != http.StatusCreated {
		t.Fatalf("register gate: %d %v", status, body)
	}
	status, body := client.do(t, http.MethodPost, "/commands", map[string]any{
		"gate_id":         "gate-fault-http",
		"target_aperture": 2.0,
		"reason":          "现场故障处置",
		"requested_by":    "dispatcher-field",
	})
	if status != http.StatusCreated {
		t.Fatalf("create command: %d %v", status, body)
	}
	commandID := mustString(t, mustField(t, body, "command"), "id")

	status, body = client.do(t, http.MethodPost, "/commands/"+commandID+"/interlock", map[string]any{
		"verdict":  "clear",
		"verifier": "safety-field",
		"note":     "条件满足",
	})
	if status != http.StatusOK {
		t.Fatalf("review: %d %v", status, body)
	}
	token := mustString(t, mustField(t, body, "command"), "execution_token")

	status, body = client.do(t, http.MethodPost, "/commands/"+commandID+"/execute", map[string]any{
		"operator":       "operator-field",
		"token":          token,
		"simulate_fault": true,
	})
	if status != http.StatusOK {
		t.Fatalf("fault execute: %d %v", status, body)
	}
	command := mustField(t, body, "command")
	gate := mustField(t, body, "gate")
	if mustString(t, command, "status") != "aborted" {
		t.Fatalf("response command status=%q, want aborted", command["status"])
	}
	if value, present := command["execution_token"]; present && value != "" {
		t.Fatalf("response still carries the credential: %v", value)
	}
	if mustString(t, gate, "status") != "faulted" {
		t.Fatalf("response gate status=%q, want faulted", gate["status"])
	}
	if value, present := gate["active_command_id"]; present && value != "" {
		t.Fatalf("faulted gate still references the finished command: %v", value)
	}

	// Dispatch-side reads must show the same terminal picture.
	status, reread := client.do(t, http.MethodGet, "/commands/"+commandID, nil)
	if status != http.StatusOK {
		t.Fatalf("reread command: %d %v", status, reread)
	}
	rereadCommand := mustField(t, reread, "command")
	if got := mustString(t, rereadCommand, "status"); got != "aborted" {
		t.Fatalf("dispatch side sees status=%q, want aborted", got)
	}
	if value, present := rereadCommand["execution_token"]; present && value != "" {
		t.Fatalf("dispatch side can still read the credential: %v", value)
	}
	status, gateReread := client.do(t, http.MethodGet, "/gates/gate-fault-http", nil)
	if mustString(t, mustField(t, gateReread, "gate"), "status") != "faulted" {
		t.Fatalf("dispatch gate status=%v, want faulted", mustField(t, gateReread, "gate")["status"])
	}

	// The credential cannot be reused after the fault.
	status, replay := client.do(t, http.MethodPost, "/commands/"+commandID+"/execute", map[string]any{
		"operator":       "operator-field",
		"token":          token,
		"simulate_fault": true,
	})
	if status != http.StatusConflict {
		t.Fatalf("token reuse after fault: status=%d body=%v, want 409", status, replay)
	}
}
