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
				events:      make([]types.SpanEvent, 0, 5),   // 事件列表，记录span执行过程中的事件，初始容量为5
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
			return make([]types.SpanEvent, 0, 5)
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

// -----------------------------
// snapshot 专用缓存池（独立于 span 池）
// -----------------------------
var (
	// snapshot 专用 attributes map 池
	snapAttrMapPool = sync.Pool{
		New: func() any {
			return make(map[string]any, 8)
		},
	}

	// snapshot 专用 events slice 池
	snapEventSlicePool = sync.Pool{
		New: func() any {
			events := make([]types.SpanEvent, 0, 8)
			return &events
		},
	}

	// snapshot 专用 logs slice 池
	snapLogSlicePool = sync.Pool{
		New: func() any {
			logs := make([]types.SpanLog, 0, 8)
			return &logs
		},
	}

	// snapshot 专用 linkedSpans slice 池
	snapLinkSlicePool = sync.Pool{
		New: func() any {
			links := make([]types.SpanContext, 0, 4)
			return &links
		},
	}
)

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
