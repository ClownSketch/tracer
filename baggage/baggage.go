package baggage

import (
	"context"
	"net/url"
	"strings"

	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/trace"
)

// Baggage 定义跨Span传播的属性
// 这是一个轻量级的传播机制，只传递必要的属性
type Baggage map[string]string

// baggageKey 用于在context中存储baggage的key
type baggageKey struct{}

// 将 baggage 添加到 context中
func WithBaggage(ctx context.Context, baggage Baggage) context.Context {
	// 如果baggage为空，返回原context
	if len(baggage) == 0 {
		return ctx
	}
	// 将baggage添加到context中
	return context.WithValue(ctx, baggageKey{}, baggage)
}

// GetBaggage 从 context 中获取 baggage
// 主要用于从 context 中获取 baggage，用于上下文传播
// 如果 baggage 不存在，返回空的 baggage
func GetBaggage(ctx context.Context) Baggage {
	// 从 context 中获取 baggage
	if baggage, ok := ctx.Value(baggageKey{}).(Baggage); ok {
		return baggage
	}
	// 如果 baggage 不存在，返回空的 baggage
	return make(Baggage)
}

// ExtractAttributeManager 从 AttributeManager 提取可传播的属性
// @param attrMgr 要提取的AttributeManager
// @return Baggage 提取的baggage
func ExtractAttributeManager(attrMgr attribute.AttributeManager) Baggage {
	// 如果 AttributeManager 为空，返回空的 Baggage
	if attrMgr == nil {
		return make(Baggage)
	}
	// 从 AttributeManager 获取属性，然后调用 ExtractFromAttributes 提取 Baggage
	return ExtractFromAttributes(attrMgr.GetInheritableAttributes(), attrMgr.GetGlobalAttributes())
}

// ExtractFromAttributes 从继承属性和全局属性中提取 Baggage
// 主要用于在创建子 span 时，从父 span 的属性中提取属性到 Baggage
// @param inheritedAttrs 继承属性
// @param globalAttrs 全局属性
// @return Baggage 提取的baggage
func ExtractFromAttributes(inheritedAttrs, globalAttrs map[string]attribute.Attribute) Baggage {
	baggage := make(Baggage)

	// 提取继承属性
	for key, attr := range inheritedAttrs {
		// 如果属性键为空，跳过
		if key == "" {
			continue
		}
		// 如果属性值为空，跳过
		val := attr.Value.String()
		if val == "" {
			continue
		}

		// 添加前缀 "i:" 表示 inheritable，i:key=value
		baggage["i:"+key] = val
	}

	// 提取全局属性
	for key, attr := range globalAttrs {
		// 如果属性键为空，跳过
		if key == "" {
			continue
		}
		// 如果属性值为空，跳过
		val := attr.Value.String()
		if val == "" {
			continue
		}

		// 添加前缀 "g:" 表示 global，g:key=value
		baggage["g:"+key] = val
	}

	return baggage
}

// RestoreToSpan 将baggage恢复到Span中
// 该方法会将baggage中的属性恢复到Span中，通过SetInheritedAttribute和SetGlobalAttribute方法
// 既会恢复到Span的属性管理器中，也会同步到Span的attributes字段
// @param baggage 要恢复的baggage
// @param span 要恢复到的Span
func RestoreToSpan(baggage Baggage, span trace.Span) {
	// 遍历baggage，将baggage恢复到Span中
	for key, val := range baggage {
		// 如果属性键长度小于3，跳过
		if len(key) < 3 {
			continue
		}
		prefix := key[:2] // 获取前缀，i: 或 g:
		rawKey := key[2:] // 获取原始键

		// 解析为 attribute.Value
		kv := attribute.String(rawKey, val)

		// 根据前缀设置到Span中
		// 使用 SetInheritedAttribute 和 SetGlobalAttribute 方法，确保属性同步到 Span 的 attributes 字段
		switch prefix {
		// 继承属性，设置到Span中
		case "i:":
			span.SetInheritedAttributes(attribute.KeyValue{Key: rawKey, Value: kv.Value})
		// 全局属性，设置到Span中
		case "g:":
			span.SetGlobalAttributes(attribute.KeyValue{Key: rawKey, Value: kv.Value})
		default:
			// 未知前缀，不处理
			continue
		}
	}
}

// ------------------------
// HTTP 传播方法
// ------------------------
func (b Baggage) ToHTTPHeader() string {
	// 如果baggage为空，返回空字符串
	if len(b) == 0 {
		return ""
	}

	// 使用 strings.Builder 直接构建整个字符串（避免 fmt.Sprintf 和中间切片）
	var builder strings.Builder
	first := true
	// 遍历baggage，将键值对添加到字符串
	for k, v := range b {
		if !first {
			builder.WriteString(",")
		}
		first = false
		// 使用 strings.Builder 拼接，避免 fmt.Sprintf
		builder.WriteString(url.QueryEscape(k))
		builder.WriteString("=")
		builder.WriteString(url.QueryEscape(v))
	}
	// 返回构建的字符串
	return builder.String()
}

// FromHTTPHeader 从HTTP头中提取baggage
func FromHTTPHeader(header string) Baggage {
	// 创建一个空的baggage
	baggage := make(Baggage)
	if header == "" {
		return baggage
	}

	// 将HTTP头按逗号分割成多个键值对
	pairs := strings.Split(header, ",")
	// 遍历键值对，将键值对添加到baggage
	for _, pair := range pairs {
		// 将键值对按等号分割成两个部分
		parts := strings.SplitN(pair, "=", 2)
		// 如果键值对分割后不是两个部分，跳过
		if len(parts) != 2 {
			continue
		}
		// 将键和值进行URL解码
		k, err1 := url.QueryUnescape(parts[0])
		v, err2 := url.QueryUnescape(parts[1])
		// 如果解码成功，将键值对添加到baggage
		if err1 == nil && err2 == nil {
			// 将键值对添加到baggage
			baggage[k] = v
		}
	}
	// 返回baggage
	return baggage
}

// IsEmpty 判断baggage是否为空
func (b Baggage) IsEmpty() bool {
	return len(b) == 0
}
