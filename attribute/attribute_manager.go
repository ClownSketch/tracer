package attribute

// AttributeType 定义属性类型类型，0 私有属性，1 全局属性，2继承属性
type AttributeType int

const (
	AttributeTypePrivate   = iota // 私有属性
	AttributeTypeGlobal           // 全局属性
	AttributeTypeInherited        // 继承属性
)

// Config 属性的附加配置
type Config struct {
	Type     AttributeType  // 属性类型
	MetaData map[string]any // 定义当前属性的一些额外信息，一般不会使用
}

// 定义属性选项，用于配置属性
type AttributeOption func(*Config)

// WithAttributeType 设置属性类型
func WithAttributeType(typ AttributeType) AttributeOption {
	return func(a *Config) {
		a.Type = typ
	}
}

// WithAttributeMetadata 设置属性元数据
func WithAttributeMetadata(metaData map[string]any) AttributeOption {
	return func(a *Config) {
		a.MetaData = metaData
	}
}

// WithMetaData 设置属性元数据
// Attribute 定义属性的完整信息
type Attribute struct {
	Key      string         // 属性键
	Value    Value          // 属性值
	Type     AttributeType  // 属性类型
	MetaData map[string]any // 定义当前属性的一些额外信息，一般不会使用
}

// AttributeManager 定义属性管理需要的接口
type AttributeManager interface {
	// 添加属性
	AddAttribute(key string, value Value, config Config)
	// 获取全局属性
	GetGlobalAttribute(key string) (Value, bool)
	GetGlobalAttributes() map[string]Attribute
	// 获取继承属性
	GetInheritedAttribute(key string) (Value, bool)
	GetInheritableAttributes() map[string]Attribute

	// 修改已有属性
	UpdateAttribute(key string, value Value, attrType AttributeType) bool
	// 删除已有属性
	RemoveAttribute(key string, attrType AttributeType) bool

	// 重置指定属性类型
	ResetType(attrType AttributeType) bool
	// 重置所有属性
	ResetAll()
}
