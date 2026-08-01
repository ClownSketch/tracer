package core

import (
	"sync"

	"github.com/ClownSketch/tracer/types"
)

var (
	// 定义 Span 状态对象池
	spanStatePool = sync.Pool{
		New: func() any {
			return &spanState{
				attributes:  make(map[string]any, 8),         // 属性映射，表示span的属性，初始容量为8
				events:      make([]spanEvent, 0, 5),         // 事件列表，记录span执行过程中的事件，初始容量为5
				eventIndex:  make(map[string]int, 5),         // 事件名称，表示事件的名称，初始容量为5
				logs:        make([]types.SpanLog, 0, 2),     // 日志列表，初始容量为2
				linkedSpans: make([]types.SpanContext, 0, 1), // 关联的子Span，初始容量为1
			}
		},
	}

	// 定义属性池
	attrMapPool = sync.Pool{
		New: func() any {
			return make(map[string]any, 8)
		},
	}
	// 定义事件池
	eventSlicePool = sync.Pool{
		New: func() any {
			return make([]spanEvent, 0, 5)
		},
	}

	// 定义事件索引池
	eventIndexMapPool = sync.Pool{
		New: func() any {
			return make(map[string]int, 5)
		},
	}

	// 定义日志池
	logSlicePool = sync.Pool{
		New: func() any {
			return make([]types.SpanLog, 0, 2)
		},
	}
)

// snapshotBuffers 保存快照复制阶段使用的内部容器。
// 四个容器共用一个池对象，避免释放快照时分别创建可逃逸的切片指针。
type snapshotBuffers struct {
	attributes  map[string]any
	events      []types.SpanEvent
	logs        []types.SpanLog
	linkedSpans []types.SpanContext
}

// snapshotBufferPool 独立管理快照容器，不与仍在写入的 Span 状态共享所有权。
var snapshotBufferPool = sync.Pool{
	New: func() any {
		return &snapshotBuffers{
			attributes:  make(map[string]any, 8),
			events:      make([]types.SpanEvent, 0, 8),
			logs:        make([]types.SpanLog, 0, 8),
			linkedSpans: make([]types.SpanContext, 0, 4),
		}
	},
}

// acquireSpanState 获取一个可复用的 Span 状态对象
func acquireSpanState() *spanState {
	state := spanStatePool.Get().(*spanState)
	state.Reset()
	return state
}

// releaseSpanState 释放当前 Span 状态对象
func releaseSpanState(state *spanState) {
	// 如果状态为空，则直接返回
	if state == nil {
		return
	}
	// 重置状态
	state.Reset()
	// 将状态放回对象池
	spanStatePool.Put(state)
}
