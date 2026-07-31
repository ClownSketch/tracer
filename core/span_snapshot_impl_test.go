package core

import (
	"testing"
	"time"

	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/types"
)

func TestCreateSnapshotInfoDeepCopiesMutableState(t *testing.T) {
	span := createSpan()
	state := span.loadState()
	if state == nil {
		t.Fatal("expected span state")
	}
	defer releaseSpanState(state)

	now := time.Now()
	span.startTime = now
	span.endTime = now.Add(10 * time.Millisecond)
	span.spanContext = types.SpanContext{
		TraceID: "trace-1",
		SpanID:  "span-1",
	}
	span.spanName = "test-span"
	span.spanKind = types.SpanKindInternal
	span.spanTraceID = "trace-1"
	span.spanParentSpanID = "parent-1"

	attrPayload := map[string]any{
		"lang": "zh",
		"tags": []any{"stable", "snapshot"},
	}
	state.attrMu.Lock()
	state.attributes["payload"] = attrPayload
	state.attributes["title"] = attribute.StringValue("hello")
	state.attrMu.Unlock()

	eventPayload := map[string]any{
		"lang": "zh",
	}
	state.eventMu.Lock()
	state.events = append(state.events, types.SpanEvent{
		Name:      "event-1",
		Timestamp: now.Format(time.RFC3339Nano),
		Attributes: map[string]any{
			"payload": eventPayload,
		},
	})
	state.eventMu.Unlock()

	logBody := map[string]any{
		"lang": "zh",
	}
	state.logMu.Lock()
	state.logs = append(state.logs, types.SpanLog{
		Timestamp: now.Format(time.RFC3339Nano),
		Message:   "log-1",
		Fields: map[string]any{
			"body": logBody,
		},
		Attributes: map[string]any{
			"env": "prod",
		},
	})
	state.logMu.Unlock()

	errDetail := &types.ErrorDetail{
		Message:         "boom",
		BusinessMessage: []string{"origin"},
		MetaData: map[string]any{
			"lang": "zh",
		},
		StackTrace: []types.StackFrame{
			{File: "origin.go", LineNumber: 1},
		},
	}
	state.errorDetail.Store(errDetail)

	resource := &types.ResourceInfo{
		ServiceName: "svc-a",
		Host:        "host-a",
		Attributes: map[string]any{
			"lang": "zh",
		},
	}
	state.resource.Store(resource)

	usage := &types.ResourceMetrics{
		CPUUsage: 1.25,
	}
	state.resourceUsage.Store(usage)

	status := &types.SpanStatus{
		Code:        types.StatusCodeOk,
		Description: "ok",
	}
	state.status.Store(status)

	snapshot := createSnapshotInfo(span, state)
	if snapshot == nil {
		t.Fatal("expected snapshot")
	}
	defer snapshot.Release()

	attrPayload["lang"] = "en"
	attrPayload["tags"].([]any)[0] = "changed"
	eventPayload["lang"] = "en"
	logBody["lang"] = "en"
	errDetail.MetaData["lang"] = "en"
	errDetail.BusinessMessage[0] = "changed"
	errDetail.StackTrace[0].File = "changed.go"
	resource.Attributes["lang"] = "en"
	usage.CPUUsage = 9.99

	attrs := snapshot.GetAttributes()
	payload, ok := attrs["payload"].(map[string]any)
	if !ok {
		t.Fatalf("expected payload map, got %T", attrs["payload"])
	}
	if got := payload["lang"]; got != "zh" {
		t.Fatalf("expected snapshot attr lang to stay zh, got %v", got)
	}
	tags, ok := payload["tags"].([]any)
	if !ok {
		t.Fatalf("expected payload tags slice, got %T", payload["tags"])
	}
	if len(tags) != 2 || tags[0] != "stable" {
		t.Fatalf("expected snapshot tags to stay stable, got %#v", tags)
	}
	if got := attrs["title"]; got != "hello" {
		t.Fatalf("expected attribute.Value to be normalized to hello, got %#v", got)
	}

	events := snapshot.GetEvents()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	evPayload, ok := events[0].Attributes["payload"].(map[string]any)
	if !ok || evPayload["lang"] != "zh" {
		t.Fatalf("expected event payload to stay frozen, got %#v", events[0].Attributes["payload"])
	}
	if events[0].Event != nil {
		t.Fatal("expected snapshot event callback to be dropped")
	}

	logs := snapshot.GetLogs()
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}
	logFields, ok := logs[0].Fields.(map[string]any)
	if !ok {
		t.Fatalf("expected log fields map, got %T", logs[0].Fields)
	}
	body, ok := logFields["body"].(map[string]any)
	if !ok || body["lang"] != "zh" {
		t.Fatalf("expected log body to stay frozen, got %#v", logs[0].Fields)
	}

	snapErr := snapshot.GetErrorDetail()
	if snapErr == nil {
		t.Fatal("expected error detail")
	}
	if got := snapErr.MetaData["lang"]; got != "zh" {
		t.Fatalf("expected error metadata to stay zh, got %v", got)
	}
	if got := snapErr.BusinessMessage[0]; got != "origin" {
		t.Fatalf("expected business message to stay origin, got %v", got)
	}
	if got := snapErr.StackTrace[0].File; got != "origin.go" {
		t.Fatalf("expected stack trace to stay origin.go, got %v", got)
	}

	snapResource := snapshot.GetResource()
	if snapResource == nil {
		t.Fatal("expected resource info")
	}
	if got := snapResource.Attributes["lang"]; got != "zh" {
		t.Fatalf("expected resource attr to stay zh, got %v", got)
	}

	snapUsage := snapshot.GetResourceUsage()
	if snapUsage == nil {
		t.Fatal("expected resource usage")
	}
	if got := snapUsage.CPUUsage; got != 1.25 {
		t.Fatalf("expected resource usage to stay 1.25, got %v", got)
	}
}
