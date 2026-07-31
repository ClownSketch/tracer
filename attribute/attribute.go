package attribute

import (
	"fmt"
	"strconv"
	"strings"
)

// ValueType 定义值类型
// 0 字符串，1 整数，2 整数64，3 浮点数32，4 浮点数64，5 布尔，6 数组
type ValueType int

const (
	TypeString  ValueType = iota // 0 字符串
	TypeInt                      // 1 整数
	TypeInt64                    // 2 整数64，int64 的别名
	TypeFloat32                  // 3 浮点数32，float32 的别名
	TypeFloat64                  // 4 浮点数64，float64 的别名
	TypeBool                     // 5 布尔，bool 的别名
	TypeArray                    // 6 数组，数组的别名
)

// Value 定义值接口
type Value interface {
	Type() ValueType // 获取值类型
	String() string  // 获取值的字符串表示
}

// ==== 基础类型实现 ====

// StringValue 字符串值
type StringValue string

// Type 获取值类型
func (s StringValue) Type() ValueType { return TypeString }

// String 获取值的字符串表示
func (s StringValue) String() string { return string(s) }

// IntValue 整数值
type IntValue int

// Type 获取值类型
func (i IntValue) Type() ValueType { return TypeInt }

// String 获取值的字符串表示
func (i IntValue) String() string { return strconv.Itoa(int(i)) }

// Int64Value 整数64值
type Int64Value int64

// Type 获取值类型
func (i Int64Value) Type() ValueType { return TypeInt64 }

// String 获取值的字符串表示
func (i Int64Value) String() string { return strconv.FormatInt(int64(i), 10) }

// Float32Value 浮点数32值
type Float32Value float32

// Type 获取值类型
func (f Float32Value) Type() ValueType { return TypeFloat32 }

// String 获取值的字符串表示
func (f Float32Value) String() string { return strconv.FormatFloat(float64(f), 'g', -1, 32) }

// Float64Value 浮点数64值
type Float64Value float64

// Type 获取值类型
func (f Float64Value) Type() ValueType { return TypeFloat64 }

// String 获取值的字符串表示
func (f Float64Value) String() string { return strconv.FormatFloat(float64(f), 'g', -1, 64) }

// BoolValue 布尔值
type BoolValue bool

// Type 获取值类型
func (b BoolValue) Type() ValueType { return TypeBool }

// String 获取值的字符串表示
func (b BoolValue) String() string {
	if b {
		return "true"
	}
	return "false"
}

// ==== 复合类型 ====

// ArrayValue 数组值
type ArrayValue []Value

// Type 获取值类型
func (a ArrayValue) Type() ValueType { return TypeArray }

// String 获取值的字符串表示
func (a ArrayValue) String() string {
	// 如果数组为空，返回空字符串
	if len(a) == 0 {
		return "[]"
	}
	// 如果数组长度大于5，返回前5个元素的字符串表示
	// 否则返回所有元素的字符串表示
	if len(a) > 5 {
		// 使用 strings.Builder 优化字符串拼接
		var b strings.Builder
		b.Grow(len(a[0].String()) + 20) // 预分配容量
		b.WriteString("[")
		b.WriteString(a[0].String())
		b.WriteString("...(")
		b.WriteString(strconv.Itoa(len(a) - 1))
		b.WriteString(" more)]")
		return b.String()
	}
	// 创建一个字符串数组，用于存储所有元素的字符串表示
	items := make([]string, len(a))
	for i, v := range a {
		items[i] = v.String()
	}
	// 返回所有元素的字符串表示，用逗号分隔
	var b strings.Builder
	b.Grow(len(items)*10 + 2) // 预分配容量
	b.WriteString("[")
	b.WriteString(strings.Join(items, ","))
	b.WriteString("]")
	return b.String()
}

// ==== KeyValue ====

// KeyValue 定义键值对
type KeyValue struct {
	Key   string // 键
	Value Value  // 值
}

// ==== 工厂函数 ====

// String 创建字符串键值对
func String(key, val string) KeyValue { return KeyValue{key, StringValue(val)} }

// Int 创建整数键值对
func Int(key string, val int) KeyValue { return KeyValue{key, IntValue(val)} }

// Int64 创建整数64键值对
func Int64(key string, val int64) KeyValue { return KeyValue{key, Int64Value(val)} }

// Float32 创建浮点数32键值对
func Float32(key string, val float32) KeyValue { return KeyValue{key, Float32Value(val)} }

// Float64 创建浮点数64键值对
func Float64(key string, val float64) KeyValue { return KeyValue{key, Float64Value(val)} }

// Bool 创建布尔键值对
func Bool(key string, val bool) KeyValue { return KeyValue{key, BoolValue(val)} }

// Array 创建一个数组类型的属性
func Array(key string, vals []Value) KeyValue { return KeyValue{key, ArrayValue(vals)} }

// ==== 动态构造 ====

// FromKeyValue 根据类型动态创建属性
// 用于从任意类型中创建属性，非必要不要使用，因为性能较差，有内存分配
func FromKeyValue(key string, val any) KeyValue {
	switch v := val.(type) {
	case string:
		return String(key, v)
	case int:
		return Int(key, v)
	case int64:
		return Int64(key, v)
	case float32:
		return Float32(key, v)
	case float64:
		return Float64(key, v)
	case bool:
		return Bool(key, v)
	case []any:
		return Array(key, parseArray(v))
	default:
		// 对于其他类型，使用 strconv 或直接类型转换
		switch v := val.(type) {
		case int8:
			return Int(key, int(v))
		case int16:
			return Int(key, int(v))
		case int32:
			return Int(key, int(v))
		case uint:
			return Int64(key, int64(v))
		case uint8:
			return Int(key, int(v))
		case uint16:
			return Int(key, int(v))
		case uint32:
			return Int64(key, int64(v))
		case uint64:
			return Int64(key, int64(v))
		case uintptr:
			return Int64(key, int64(v))
		default:
			// 最后使用 strconv.FormatFloat 处理数字类型，或直接转换
			return String(key, toString(val))
		}
	}
}

// parseArray 将 any 类型的切片解析为 []Value
func parseArray(arr []any) []Value {
	values := make([]Value, 0, len(arr))
	for _, item := range arr {
		values = append(values, FromKeyValue("", item).Value)
	}
	return values
}

// toString 将任意类型转换为字符串，避免使用 fmt.Sprintf
func toString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case int:
		return strconv.Itoa(val)
	case int8:
		return strconv.FormatInt(int64(val), 10)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint:
		return strconv.FormatUint(uint64(val), 10)
	case uint8:
		return strconv.FormatUint(uint64(val), 10)
	case uint16:
		return strconv.FormatUint(uint64(val), 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case uintptr:
		return strconv.FormatUint(uint64(val), 10)
	case float32:
		return strconv.FormatFloat(float64(val), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(val, 'g', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		// 对于无法直接转换的类型，使用 fmt.Sprintf 作为最后手段
		// 但这种情况应该很少见
		return fmt.Sprintf("%v", v)
	}
}
