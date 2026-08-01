package core

import (
	"context"
	"time"

	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/baggage"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/trace/noop"
	"github.com/ClownSketch/tracer/types"
	"github.com/ClownSketch/tracer/utils"
)

// tracerImpl 是 Tracer 的实现
type tracerImpl struct {
	tracerName     string               // 追踪器名称
	tracerProvider trace.TracerProvider // 追踪器提供者
	spanProcessor  trace.SpanProcessor  // 跨度处理器
	sampler        trace.SpanSampler    // 跨度采样器
	resource       *types.ResourceInfo  // 默认资源信息
}

// NewTracerImpl 创建一个新的 TracerImpl 实例
func NewTracerImpl(tracerName string, tracerProvider trace.TracerProvider, spanProcessor trace.SpanProcessor, sampler trace.SpanSampler) trace.Tracer {
	return NewTracerImplWithResource(tracerName, tracerProvider, spanProcessor, sampler, nil)
}

// NewTracerImplWithResource 创建带默认资源信息的 Tracer。
func NewTracerImplWithResource(tracerName string, tracerProvider trace.TracerProvider, spanProcessor trace.SpanProcessor, sampler trace.SpanSampler, resource *types.ResourceInfo) trace.Tracer {
	return &tracerImpl{
		tracerName:     tracerName,
		tracerProvider: tracerProvider,
		spanProcessor:  spanProcessor,
		sampler:        sampler,
		resource:       cloneResourceInfo(resource),
	}
}

func (t *tracerImpl) Start(ctx context.Context, spanName string, options ...types.SpanOptions) (context.Context, trace.Span) {
	// 初始化 Span 配置
	spanConfig := &types.SpanConfig{}
	// 应用Span配置选项
	for _, option := range options {
		option(spanConfig)
	}

	// 优先从 context 中获取从 Extract 返回的 SpanContext（跨服务传播）
	// 这样可以确保从上游服务提取的 SpanContext 被正确使用
	parentSpanContext := baggage.SpanContextFromContext(ctx)

	// 获取父 Span（用于后续的继承属性等操作）
	parentSpan := baggage.GetSpanContext(ctx)

	// 如果 context 中没有从 Extract 返回的 SpanContext，则尝试从父 Span 中获取
	// 这是正常的父子关系（同一服务内的 Span 关系）
	if !parentSpanContext.Validate() {
		// 从 parentSpan 父 Span 中获取父 Span 的上下文
		parentSpanContext = parentSpan.SpanContext()
	}
	// 获取父Span的TraceID
	traceId := parentSpanContext.TraceID
	if traceId == "" {
		// 如果TraceID为空，则生成新的TraceID
		traceId = utils.GenTraceID()
	}

	// 确定 ParentSpanID
	// 如果 parentSpanContext 是从 Extract 返回的（Remote == true），
	// 则 ParentSpanID 存储了发送方的 spanID，应该直接使用
	// 如果 parentSpanContext 是从 Extract 返回的但 Remote == false（Extract 自己生成的，断链），
	// 则 ParentSpanID 应该为空（当前 span 是根 Span）
	// 否则，如果是正常的父子关系（同服务内，从 parentSpan 获取），则使用 SpanID（父 Span 的 SpanID 就是当前 Span 的 ParentSpanID）
	parentSpanID := parentSpanContext.ParentSpanID
	// 如果 ParentSpanID 为空，需要判断是断链还是正常的父子关系
	if parentSpanID == "" {
		// 检查 parentSpanContext 是否是从 Extract 返回的（在 context 中存在）
		extractedSpanContext := baggage.SpanContextFromContext(ctx)
		if extractedSpanContext.Validate() && !extractedSpanContext.Remote {
			// Extract 自己生成的 SpanContext（断链），当前 span 是根 Span，ParentSpanID 应该为空
			parentSpanID = ""
		} else if parentSpanContext.SpanID != "" {
			// 正常的父子关系（同服务内），父 Span 的 SpanID 就是当前 Span 的 ParentSpanID
			parentSpanID = parentSpanContext.SpanID
		}
	}

	// 创建一个新的SpanContext
	spanContext := types.SpanContext{
		// 设置TraceID，如果父Span的上下文有效，则使用父Span的TraceID，否则生成新的TraceID
		TraceID: traceId,
		// 设置SpanID，这里使用新的SpanID
		SpanID: utils.GenSpanID(),
		// 设置ParentSpanID，优先使用 ParentSpanID（从 Extract 返回），否则使用 SpanID（父子关系）
		ParentSpanID: parentSpanID,
		// 设置TraceFlags，根据配置决定是否记录
		TraceFlags: parentSpanContext.TraceFlags,
	}

	samplingResult := types.SamplingResult{Decision: types.SamplingDecisionRecordAndSample}
	if parentSpanContext.Validate() {
		if parentSpanContext.TraceFlags&types.TraceFlagsSampled == 0 {
			samplingResult.Decision = types.SamplingDecisionDrop
		}
	} else if t.sampler != nil {
		samplingResult = t.sampler.ShouldSample(
			types.SamplingParameters{
				TraceID:      spanContext.TraceID,
				ParentSpanID: spanContext.ParentSpanID,
				Name:         spanName,
				SpanKind:     spanConfig.SpanKind,
			},
		)
	}

	recordPolicy := resolveRecordPolicyAtStart(spanConfig.ForceRecord, samplingResult.Decision)
	if samplingResult.Decision == types.SamplingDecisionDrop && recordPolicy == types.RecordPolicyNone {
		return ctx, new(noop.NoopSpan)
	}
	if parentSpanContext.Validate() {
		spanContext.TraceFlags = parentSpanContext.TraceFlags
	} else if samplingResult.Decision == types.SamplingDecisionRecordAndSample || recordPolicy == types.RecordPolicyAlways {
		spanContext.TraceFlags |= types.TraceFlagsSampled
	} else {
		spanContext.TraceFlags &^= types.TraceFlagsSampled
	}

	// 创建一个新的 Span 实现
	span := createSpan()
	span.startTime = time.Now()          // 设置Span开始时间
	span.spanName = spanName             // 设置当前Span名称
	span.spanKind = spanConfig.SpanKind  // 设置当前Span类型
	span.spanTraceID = traceId           // 设置当前Span的TraceID
	span.spanParentSpanID = parentSpanID // 设置当前Span的ParentSpanID
	span.spanContext = spanContext       // 设置当前Span的上下文

	spanState := span.loadState()
	spanState.tracerImpl = t // 设置当前Span的所属追踪器
	spanState.forceRecord.Store(recordPolicy)
	if t.resource != nil {
		spanState.resource.Store(cloneResourceInfo(t.resource))
	}

	// 如果是远程传播（跨服务），从 context 中的 baggage 恢复属性
	// 这应该在从父 Span 获取属性之前执行，因为 baggage 可能包含从上游服务传递的属性
	// 性能优化：只对远程传播执行 baggage 恢复，减少本地传播时的判断损耗
	if parentSpanContext.Validate() && parentSpanContext.Remote {
		extractedBaggage := baggage.GetBaggage(ctx)
		if len(extractedBaggage) > 0 {
			// 将 baggage 恢复到 Span 中（会同步到属性管理器和 attributes 字段）
			baggage.RestoreToSpan(extractedBaggage, span)
		}
	}

	// 判断父Span是否存在，并且父Span上下文合法
	if parentSpan != nil {
		// 从父Span中获取MongoDB导出目标集合名
		if coll := parentSpan.GetMongoCollection(); coll != "" {
			span.SetMongoCollection(coll)
		}
		// 从父Span中获取继承和全局数据
		inheritedAttributes := parentSpan.GetInheritedAttributes() // 获取继承属性
		// 遍历继承属性，设置到当前Span
		for _, v := range inheritedAttributes {
			span.SetInheritedAttributes(attribute.KeyValue{
				Key:   v.Key,
				Value: v.Value,
			})
		}
		globalAttributes := parentSpan.GetGlobalAttributes() // 获取全局属性
		// 遍历全局属性，设置到当前Span
		for _, v := range globalAttributes {
			span.SetGlobalAttributes(attribute.KeyValue{
				Key:   v.Key,
				Value: v.Value,
			})
		}
	}

	// 如果Span配置中设置了MongoDB导出目标集合名，则设置到当前Span中
	if spanConfig.MongoCollection != "" {
		span.SetMongoCollection(spanConfig.MongoCollection)
	}

	// 设置采样器决定的属性
	for k, v := range samplingResult.Attributes {
		span.SetAttributes(attribute.FromKeyValue(k, v))
	}

	// 通知Span处理器，Span开始
	if t.spanProcessor != nil {
		t.spanProcessor.OnStart(ctx, span)
	}

	// 将新Span添加到现有上下文中，保持上下文继承关系
	newCtx := baggage.WithSpanContext(ctx, span)

	return newCtx, span
}
