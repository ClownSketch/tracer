package baggage

import (
	"sync"

	"github.com/ClownSketch/tracer/attribute"
)

// AttributeManager 定义属性管理器
// 定义一个读优先的属性管理器
type AttributeManager struct {
	inheritableMu    sync.RWMutex
	inheritableAttrs map[string]attribute.Attribute
	globalMu         sync.RWMutex
	globalAttrs      map[string]attribute.Attribute
}

// NewAttributeManager 创建新的属性管理器
func NewAttributeManager() attribute.AttributeManager {
	return &AttributeManager{}
}

// AddAttribute 添加属性，属性类型根据 config 进行定义
// 全局属性：config.Type = attribute.AttributeTypeGlobal
// 继承属性：config.Type = attribute.AttributeTypeInherited
func (bm *AttributeManager) AddAttribute(key string, value attribute.Value, config attribute.Config) {
	// 判断属性键或者值是否为空
	if key == "" || value == nil {
		return
	}

	// 创建属性
	attr := attribute.Attribute{
		Key:      key,
		Value:    value,
		Type:     config.Type,
		MetaData: config.MetaData,
	}

	// 根据属性类型进行添加
	switch config.Type {
	case attribute.AttributeTypeInherited:
		bm.inheritableMu.Lock()
		if bm.inheritableAttrs == nil {
			bm.inheritableAttrs = make(map[string]attribute.Attribute, 4)
		}
		bm.inheritableAttrs[key] = attr
		bm.inheritableMu.Unlock()
	case attribute.AttributeTypeGlobal:
		bm.globalMu.Lock()
		if bm.globalAttrs == nil {
			bm.globalAttrs = make(map[string]attribute.Attribute, 4)
		}
		bm.globalAttrs[key] = attr
		bm.globalMu.Unlock()
	}
}

// GetGlobalAttribute 获取全局属性
// 如果属性不存在，返回 false
// 如果属性存在，返回 true 和属性值
func (bm *AttributeManager) GetGlobalAttribute(key string) (attribute.Value, bool) {
	bm.globalMu.RLock()         // 加读锁
	defer bm.globalMu.RUnlock() // 解锁

	attr, ok := bm.globalAttrs[key]
	// 如果属性不存在，返回 false
	if !ok {
		return nil, false
	}
	// 如果属性存在，返回 true 和属性值
	return attr.Value, true
}

// GetGlobalAttributes 获取全局属性
// 这里返回的是一个拷贝，只读，防止外部修改
func (bm *AttributeManager) GetGlobalAttributes() map[string]attribute.Attribute {
	bm.globalMu.RLock()
	defer bm.globalMu.RUnlock()

	if len(bm.globalAttrs) == 0 {
		return nil
	}

	newMap := make(map[string]attribute.Attribute, len(bm.globalAttrs))
	for k, v := range bm.globalAttrs {
		newMap[k] = v
	}
	return newMap
}

// GetInheritedAttribute 获取继承属性
// 如果属性不存在，返回 false
// 如果属性存在，返回 true 和属性值
func (bm *AttributeManager) GetInheritedAttribute(key string) (attribute.Value, bool) {
	bm.inheritableMu.RLock()         // 加读锁
	defer bm.inheritableMu.RUnlock() // 解锁

	attr, ok := bm.inheritableAttrs[key]
	// 如果属性不存在，返回 false
	if !ok {
		return nil, false
	}
	// 如果属性存在，返回 true 和属性值
	return attr.Value, true
}

// GetInheritableAttributes 获取继承属性
// 这里返回的是一个拷贝，只读，防止外部修改
func (bm *AttributeManager) GetInheritableAttributes() map[string]attribute.Attribute {
	bm.inheritableMu.RLock()
	defer bm.inheritableMu.RUnlock()

	if len(bm.inheritableAttrs) == 0 {
		return nil
	}

	newMap := make(map[string]attribute.Attribute, len(bm.inheritableAttrs))
	for k, v := range bm.inheritableAttrs {
		newMap[k] = v
	}
	return newMap
}

// UpdateAttribute 修改属性
// 如果属性不存在，返回 false，表示修改失败
// 如果属性存在，返回 true
func (bm *AttributeManager) UpdateAttribute(key string, value attribute.Value, attrType attribute.AttributeType) bool {
	if key == "" || value == nil {
		return false
	}
	switch attrType {
	case attribute.AttributeTypeInherited:
		bm.inheritableMu.Lock()
		defer bm.inheritableMu.Unlock()
		current, exists := bm.inheritableAttrs[key]
		if !exists {
			return false
		}
		current.Value = value
		bm.inheritableAttrs[key] = current
		return true
	case attribute.AttributeTypeGlobal:
		bm.globalMu.Lock()
		defer bm.globalMu.Unlock()
		current, exists := bm.globalAttrs[key]
		if !exists {
			return false
		}
		current.Value = value
		bm.globalAttrs[key] = current
		return true
	default:
		return false
	}
}

// RemoveAttribute 删除属性
// 如果属性不存在，返回 false，表示删除失败
// 如果属性存在，返回 true
func (bm *AttributeManager) RemoveAttribute(key string, attrType attribute.AttributeType) bool {
	if key == "" {
		return false
	}
	switch attrType {
	case attribute.AttributeTypeInherited:
		bm.inheritableMu.Lock()
		defer bm.inheritableMu.Unlock()
		if _, exists := bm.inheritableAttrs[key]; !exists {
			return false
		}
		delete(bm.inheritableAttrs, key)
		return true
	case attribute.AttributeTypeGlobal:
		bm.globalMu.Lock()
		defer bm.globalMu.Unlock()
		if _, exists := bm.globalAttrs[key]; !exists {
			return false
		}
		delete(bm.globalAttrs, key)
		return true
	default:
		return false
	}
}

// ResetType 重置属性类型
// 如果属性类型不存在，返回 false，表示重置失败
// 如果属性类型存在，返回 true
func (bm *AttributeManager) ResetType(attrType attribute.AttributeType) bool {
	switch attrType {
	case attribute.AttributeTypeInherited:
		bm.inheritableMu.Lock()
		clear(bm.inheritableAttrs)
		bm.inheritableMu.Unlock()
		return true
	case attribute.AttributeTypeGlobal:
		bm.globalMu.Lock()
		clear(bm.globalAttrs)
		bm.globalMu.Unlock()
		return true
	}
	return false
}

// ResetAll 重置属性管理器
func (bm *AttributeManager) ResetAll() {
	bm.inheritableMu.Lock()
	clear(bm.inheritableAttrs)
	bm.inheritableMu.Unlock()

	bm.globalMu.Lock()
	clear(bm.globalAttrs)
	bm.globalMu.Unlock()
}
