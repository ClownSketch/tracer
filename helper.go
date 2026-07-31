package tracer

import (
	"github.com/ClownSketch/tracer/types"
	"github.com/ClownSketch/tracer/types/operation"
)

// ==================== Build 方法 ====================
//
// 这些方法用于构建事件数据，可以直接在 tracer 包中使用

// BuildSQLOperationEvent 构建 SQL 操作事件
// 返回一个函数，该函数在被调用时返回包含 SQL 操作信息的 map
// 使用示例：
//
//	sqlInfo := &operation.SQLOperationInfo{...}
//	span.AddEvent("sql.operation", "sql", tracer.BuildSQLOperationEvent(sqlInfo))
func BuildSQLOperationEvent(sqlInfo *operation.SQLOperationInfo) func() map[string]any {
	return func() map[string]any {
		if sqlInfo == nil {
			return nil
		}

		return map[string]any{
			"table":        sqlInfo.Table,
			"operation":    sqlInfo.Operation,
			"rows":         sqlInfo.Rows,
			"sql":          sqlInfo.SQL,
			"stack":        sqlInfo.Stack,
			"message":      sqlInfo.Message,
			"cost_seconds": sqlInfo.CostSeconds,
			"success":      sqlInfo.Success,
			"transaction":  sqlInfo.Transaction,
			"timestamp":    sqlInfo.Timestamp,
		}
	}
}

// BuildRedisEvent 构建 Redis 操作事件
// 返回一个函数，该函数在被调用时返回包含 Redis 操作信息的 map
// 使用示例：
//
//	redisInfo := &operation.RedisOperationInfo{...}
//	span.AddEvent("redis.operation", "redis", tracer.BuildRedisEvent(redisInfo))
func BuildRedisEvent(redis *operation.RedisOperationInfo) func() map[string]any {
	return func() map[string]any {
		if redis == nil {
			return nil
		}

		return map[string]any{
			"index_db":     redis.IndexDb,
			"operation":    redis.Operation,
			"ttl":          redis.TTL,
			"key":          redis.Key,
			"value":        redis.Value,
			"cost_seconds": redis.CostSeconds,
			"success":      redis.Success,
			"transaction":  redis.Transaction,
			"pipeline":     redis.Pipeline,
			"stack":        redis.Stack,
			"message":      redis.Message,
			"timestamp":    redis.Timestamp,
		}
	}
}

// BuildRequestEvent 构建请求事件
// 返回一个函数，该函数在被调用时返回包含请求信息的 map
// 使用示例：
//
//	reqInfo := &operation.RequestInfo{...}
//	span.AddEvent("http.request", "http", tracer.BuildRequestEvent(reqInfo))
func BuildRequestEvent(req *operation.RequestInfo) func() map[string]any {
	return func() map[string]any {
		if req == nil {
			return nil
		}

		result := map[string]any{
			"ttl":          req.TTL,
			"method":       req.Method,
			"decoded_url":  req.DecodedURL,
			"headers":      req.Headers,
			"body":         req.Body,
			"client_ip":    req.ClientIP,
			"user_agent":   req.UserAgent,
			"timestamp":    req.Timestamp,
			"cost_seconds": req.CostSeconds,
		}
		if req.QueryString != "" {
			result["query_string"] = req.QueryString
		}
		return result
	}
}

// BuildResponseEvent 构建响应事件
// 返回一个函数，该函数在被调用时返回包含响应信息的 map
// 使用示例：
//
//	respInfo := &operation.ResponseInfo{...}
//	span.AddEvent("http.response", "http", tracer.BuildResponseEvent(respInfo))
func BuildResponseEvent(resp *operation.ResponseInfo) func() map[string]any {
	return func() map[string]any {
		if resp == nil {
			return nil
		}

		return map[string]any{
			"retry_id":          resp.RetryID,
			"method":            resp.Method,
			"header":            resp.Header,
			"body":              resp.Body,
			"business_code":     resp.BusinessCode,
			"business_code_msg": resp.BusinessCodeMsg,
			"http_code":         resp.HttpCode,
			"http_code_msg":     resp.HttpCodeMsg,
			"cost_seconds":      resp.CostSeconds,
			"timestamp":         resp.Timestamp,
		}
	}
}

// BuildExternalCallInfoEvent 构建外部调用信息事件
// 返回一个函数，该函数在被调用时返回包含外部调用信息的 map
// 使用示例：
//
//	externalCallInfo := &operation.ExternalCallInfo{...}
//	span.AddEvent("external.call", "external", tracer.BuildExternalCallInfoEvent(externalCallInfo))
func BuildExternalCallInfoEvent(externalCallInfo *operation.ExternalCallInfo) func() map[string]any {
	return func() map[string]any {
		if externalCallInfo == nil {
			return nil
		}

		var req map[string]any
		if externalCallInfo.Request != nil {
			req = BuildRequestEvent(externalCallInfo.Request)()
		}

		var res []map[string]any
		if len(externalCallInfo.Response) > 0 {
			res = responsesToMaps(externalCallInfo.Response)
		}

		return map[string]any{
			"service_name": externalCallInfo.ServiceName,
			"span_id":      externalCallInfo.SpanID,
			"trace_id":     externalCallInfo.TraceID,
			"caller_name":  externalCallInfo.CallerName,
			"request":      req,
			"response":     res,
			"success":      externalCallInfo.Success,
			"cost_seconds": externalCallInfo.CostSeconds,
			"fail_count":   externalCallInfo.FailCount,
			"fail_reason":  externalCallInfo.FailReason,
			"is_external":  externalCallInfo.IsExternal,
		}
	}
}

// BuildErrorEvent 构建错误事件
// 返回一个函数，该函数在被调用时返回包含错误信息的 map
// 使用示例：
//
//	errorDetail := &tracer.ErrorDetail{...}
//	span.AddEvent("error", "error", tracer.BuildErrorEvent(errorDetail))
func BuildErrorEvent(err *types.ErrorDetail) func() map[string]any {
	return func() map[string]any {
		if err == nil {
			return nil
		}

		var stackTrace any
		if len(err.StackTrace) > 0 {
			stackTrace = err.StackTrace
		}

		return map[string]any{
			"code":             err.Code,
			"message":          err.Message,
			"business_code":    err.BusinessCode,
			"business_message": err.BusinessMessage,
			"http_code":        err.HttpCode,
			"timestamp":        err.Timestamp,
			"stack_trace":      stackTrace,
			"meta_data":        err.MetaData,
		}
	}
}

// BuildLogEvent 构建日志事件
// 返回一个函数，该函数在被调用时返回包含日志信息的 map
// 使用示例：
//
//	log := &types.SpanLog{
//		Timestamp:  "2021-01-01T00:00:00.000Z",
//		Message:    "这是一条日志消息",
//		Severity:   types.SpanLogSeverityInfo,
//		EventType:  "log",
//		Fields:     map[string]any{"key": "value"},
//		Attributes: map[string]any{"attr": "value"},
//	}
//	span.AddEvent("log", "log", tracer.BuildLogEvent(log))
func BuildLogEvent(log *types.SpanLog) func() map[string]any {
	return func() map[string]any {
		if log == nil {
			return nil
		}

		result := map[string]any{
			"timestamp": log.Timestamp,
			"message":   log.Message,
			"severity":  log.Severity.String(),
		}

		// 只有当字段不为空时才添加
		if log.Fields != nil {
			result["fields"] = log.Fields
		}

		// 只有当事件类型不为空时才添加
		if log.EventType != "" {
			result["event_type"] = log.EventType
		}

		// 只有当属性不为空时才添加
		if len(log.Attributes) > 0 {
			result["attributes"] = log.Attributes
		}

		return result
	}
}

// responsesToMaps 将响应信息数组转换为 map 数组
func responsesToMaps(response []*operation.ResponseInfo) []map[string]any {
	if len(response) == 0 {
		return nil
	}
	result := make([]map[string]any, 0, len(response))
	for _, resp := range response {
		if resp == nil {
			continue
		}
		result = append(result, BuildResponseEvent(resp)())
	}
	return result
}
