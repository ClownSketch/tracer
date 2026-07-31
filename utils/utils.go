package utils

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// GetTimestamp 获取当前时间戳
func GetTimestamp() string {
	return time.Now().Format(time.RFC3339)
}

// GenTraceID 生成 W3C 追踪 ID
func GenTraceID() string {
	id := make([]byte, 16)        // 定义16字节的字节数组
	_, _ = rand.Read(id)          // 生成随机字节，忽略错误
	return hex.EncodeToString(id) // 将字节数组转换为十六进制字符串，长度为32
}

// GenSpanID 生成 W3C 跨度 ID
func GenSpanID() string {
	id := make([]byte, 8)         // 定义8字节的字节数组
	_, _ = rand.Read(id)          // 生成随机字节，忽略错误
	return hex.EncodeToString(id) // 将8字节的字节数组转换为16进制字符串，长度为16
}
