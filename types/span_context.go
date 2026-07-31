package types

const TraceFlagsSampled uint8 = 0x01

// SpanContext 定义Span跨度上下文，用于传递Span需要的上下文信息
type SpanContext struct {
	TraceID      string // 追踪器ID，16进制字符串，长度为32
	SpanID       string // SpanID，16进制字符串，长度为16
	ParentSpanID string // 父SpanID，16进制字符串，长度为16，为空表示根Span
	TraceFlags   uint8  // 追踪器标志，0表示没有标志，1表示有标志
	TraceState   string // 追踪器状态，16进制字符串，长度为16
	Remote       bool   // 是否远程，true表示远程，false表示本地
}

// 验证SpanContext是否有效
func (sc SpanContext) Validate() bool {
	if !isValidHexID(sc.TraceID, 32) {
		return false
	}
	if sc.Remote {
		return isValidHexID(sc.ParentSpanID, 16)
	}
	if !isValidHexID(sc.SpanID, 16) {
		return false
	}
	return sc.ParentSpanID == "" || isValidHexID(sc.ParentSpanID, 16)
}

func isValidHexID(value string, expectedLength int) bool {
	if len(value) != expectedLength {
		return false
	}
	nonZero := false
	for index := 0; index < len(value); index++ {
		char := value[index]
		if char != '0' {
			nonZero = true
		}
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return nonZero
}
