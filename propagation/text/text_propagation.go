package text

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/ClownSketch/tracer/baggage"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

var (
	// ErrorSpanNotFound 表示Span不存在
	ErrorSpanNotFound = errors.New("span not found in context")

	// ErrorInvalidSpan 表示Span无效
	ErrorInvalidSpan = errors.New("invalid span")

	// ErrorInvalidTraceID 表示TraceID无效
	ErrorInvalidTraceID = errors.New("invalid trace id")

	// ErrorInvalidSpanID 表示SpanID无效
	ErrorInvalidSpanID = errors.New("invalid span id")

	// ErrorTraceContextLost 表示追踪上下文丢失
	ErrorTraceContextLost = errors.New("trace context lost, created orphan span")
)

const (
	// TraceParentHeaderKey 表示TraceParent头键
	TraceParentHeaderKey = "traceparent"

	// TraceStateHeaderKey 表示TraceState头键
	TraceStateHeaderKey = "tracestate"

	// BaggageHeaderKey 表示Baggage头键（用于跨服务传播属性）
	BaggageHeaderKey = "baggage"

	// 自定义 Trace ID 头（降级方案）
	CustomTraceIDHeaderKey    = "X-Trace-ID"
	CustomSpanIDHeaderKey     = "X-Span-ID"
	CustomTraceFlagsHeaderKey = "X-Trace-Flags"
)

// TextMapPropagator 是一个文本映射传播器实现
type TextMapPropagator struct {
	carrier *TextMapCarrier
}

// NewTextMapPropagator 创建一个新的 TextMapPropagator
func NewTextMapPropagator() *TextMapPropagator {
	return &TextMapPropagator{carrier: NewTextMapCarrier()}
}

// Extract 从文本映射中提取 Span 上下文和 baggage
// 实现 Propagator 接口
// 返回更新后的 context（包含提取的 SpanContext 和 Baggage）
func (p *TextMapPropagator) Extract(ctx context.Context, carrier trace.ContextCarrier) context.Context {
	var spanCtx types.SpanContext
	traceParent := carrier.Get(TraceParentHeaderKey)

	if traceParent == "" {
		customTraceID := carrier.Get(CustomTraceIDHeaderKey)
		customSpanID := carrier.Get(CustomSpanIDHeaderKey)
		if customTraceID != "" && customSpanID != "" {
			traceFlags := types.TraceFlagsSampled
			validFlags := true
			if flagsValue := carrier.Get(CustomTraceFlagsHeaderKey); flagsValue != "" {
				if parsedFlags, err := strconv.ParseUint(flagsValue, 16, 8); err == nil && len(flagsValue) == 2 {
					traceFlags = uint8(parsedFlags)
				} else {
					validFlags = false
				}
			}
			candidate := types.SpanContext{
				TraceID:      customTraceID,
				ParentSpanID: customSpanID,
				TraceFlags:   traceFlags,
				Remote:       true,
			}
			if validFlags && candidate.Validate() {
				spanCtx = candidate
			}
		}
	} else {
		parts := strings.Split(traceParent, "-")
		if len(parts) == 4 && parts[0] == "00" && len(parts[3]) == 2 {
			tracerID, parentSpanID, traceFlags := parts[1], parts[2], parts[3]
			traceFlagsInt, err := strconv.ParseUint(traceFlags, 16, 8)
			if err == nil {
				candidate := types.SpanContext{
					TraceID:      tracerID,
					ParentSpanID: parentSpanID,
					TraceFlags:   uint8(traceFlagsInt),
					TraceState:   carrier.Get(TraceStateHeaderKey),
					Remote:       true,
				}
				if candidate.Validate() {
					spanCtx = candidate
				}
			}
		}
	}

	// 提取 baggage
	baggageHeader := carrier.Get(BaggageHeaderKey)
	if baggageHeader != "" {
		// 从 HTTP header 中解析 baggage
		extractedBaggage := baggage.FromHTTPHeader(baggageHeader)
		if len(extractedBaggage) > 0 {
			// 将 baggage 添加到 context 中
			ctx = baggage.WithBaggage(ctx, extractedBaggage)
		}
	}

	// 将 SpanContext 添加到 context 中
	ctx = baggage.WithContextSpanContext(ctx, spanCtx)

	return ctx
}

// Inject 将 Span 上下文注入到文本映射中
// 实现 Propagator 接口
func (p *TextMapPropagator) Inject(ctx context.Context, carrier trace.ContextCarrier) error {
	// 从上下文中获取当前 Span
	span := baggage.GetSpanContext(ctx)
	// 如果 span 不存在，则返回错误
	if span == nil {
		// 返回 Span 不存在错误
		return ErrorSpanNotFound
	}

	// 获取当前 Span 的 SpanContext
	spanContext := span.SpanContext()
	// 如果 SpanContext 无效，则返回错误
	if !spanContext.Validate() {
		// 返回 SpanContext 无效错误
		return ErrorInvalidSpan
	}

	// 构建标准 W3C traceparent（使用 strings.Builder 优化性能）
	var b strings.Builder
	// 预分配容量: "00-" (3) + TraceID (32) + "-" (1) + SpanID (16) + "-" (1) + TraceFlags (2) = 55
	b.Grow(55)
	b.WriteString("00-")
	b.WriteString(spanContext.TraceID)
	b.WriteString("-")
	b.WriteString(spanContext.SpanID)
	b.WriteString("-")
	// 格式化 TraceFlags 为两位十六进制（避免使用 fmt.Sprintf）
	flagsHex := strconv.FormatUint(uint64(spanContext.TraceFlags), 16)
	customFlagsHex := flagsHex
	if len(flagsHex) == 1 {
		b.WriteString("0") // 补零
		customFlagsHex = "0" + flagsHex
	}
	b.WriteString(flagsHex)
	traceParent := b.String()

	// 注入 SpanContext
	carrier.Set(TraceParentHeaderKey, traceParent)
	// 同时注入降级方案
	carrier.Set(CustomTraceIDHeaderKey, spanContext.TraceID)
	carrier.Set(CustomSpanIDHeaderKey, spanContext.SpanID)
	carrier.Set(CustomTraceFlagsHeaderKey, customFlagsHex)
	// 注入 tracestate
	if spanContext.TraceState != "" {
		// 注入 tracestate
		carrier.Set(TraceStateHeaderKey, spanContext.TraceState)
	}

	// 注入 Baggage
	// 首先尝试从 context 中获取 baggage
	bg := baggage.GetBaggage(ctx)
	// 如果 context 中没有 baggage，则从 Span 的属性中自动提取 baggage
	if len(bg) == 0 {
		// 从 Span 的继承属性和全局属性中提取 baggage
		inheritedAttrs := span.GetInheritedAttributes()
		globalAttrs := span.GetGlobalAttributes()
		bg = baggage.ExtractFromAttributes(inheritedAttrs, globalAttrs)
	}
	// 如果 baggage 不为空，则注入到 carrier 中
	if len(bg) > 0 {
		// 将 baggage 转换为 HTTP 头
		baggageHeader := bg.ToHTTPHeader()
		if baggageHeader != "" {
			carrier.Set(BaggageHeaderKey, baggageHeader)
		}
	}

	// 返回 nil
	return nil
}
