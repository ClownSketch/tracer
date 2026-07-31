package core

import "github.com/ClownSketch/tracer/trace"

// SnapshotSerializer 可选接口：用于将 SpanSnapshot 序列化到磁盘、从磁盘反序列化回 SpanSnapshot
type SnapshotSerializer interface {
	Marshal(trace.SpanSnapshot) ([]byte, error)
	Unmarshal([]byte) (trace.SpanSnapshot, error)
}
