package baggage

import (
	"sync"
	"sync/atomic"

	"github.com/ClownSketch/tracer/attribute"
)

// AttributeManager 定义属性管理器
// 定义一个读优先的属性管理器
type AttributeManager struct {
	inheritableMu    sync.RWMutex // 继承属性读写锁，只保护写（copy-on-write）
	inheritableAttrs atomic.Value // 继承属性 存 map[string]attribute.Attribute，使用atomic.Value 保证原子性
	globalMu         sync.RWMutex // 全局属性读写锁，只保护写（copy-on-write）
	globalAttrs      atomic.Value // 全局属性 存 map[string]attribute.Attribute，使用atomic.Value 保证原子性
}

// NewAttributeManager 创建新的属性管理器
// 初始化并存入空 map 快照
func NewAttributeManager() attribute.AttributeManager {
	bm := &AttributeManager{}
	// 初始化并存入空 map 快照
	bm.inheritableAttrs.Store(make(map[string]attribute.Attribute))
	// 初始化并存入空 map 快照
	bm.globalAttrs.Store(make(map[string]attribute.Attribute))

	// 返回属性管理器
	return bm
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
	// 继承属性
	case attribute.AttributeTypeInherited:
		bm.inheritableMu.Lock()         // 加写锁
		defer bm.inheritableMu.Unlock() // 解锁

		// 获取私有属性 map 快照
		old := bm.inheritableAttrs.Load().(map[string]attribute.Attribute)
		// 创建新的 map，并复制旧的 map 数据
		newMap := make(map[string]attribute.Attribute, len(old)+1)
		// 遍历旧的 map，并复制数据到新的 map
		for k, v := range old {
			// 复制数据
			newMap[k] = v
		}
		// 添加新的属性
		newMap[key] = attr
		// 存入新的 map
		bm.inheritableAttrs.Store(newMap)

	// 全局属性
	case attribute.AttributeTypeGlobal:
		bm.globalMu.Lock()         // 加写锁
		defer bm.globalMu.Unlock() // 解锁

		// 获取私有属性 map 快照
		old := bm.globalAttrs.Load().(map[string]attribute.Attribute)
		// 创建新的 map，并复制旧的 map 数据
		newMap := make(map[string]attribute.Attribute, len(old)+1)
		// 遍历旧的 map，并复制数据到新的 map
		for k, v := range old {
			// 复制数据
			newMap[k] = v
		}
		// 添加新的属性
		newMap[key] = attr
		// 存入新的 map
		bm.globalAttrs.Store(newMap)

	}
}

// GetGlobalAttribute 获取全局属性
// 如果属性不存在，返回 false
// 如果属性存在，返回 true 和属性值
func (bm *AttributeManager) GetGlobalAttribute(key string) (attribute.Value, bool) {
	bm.globalMu.RLock()         // 加读锁
	defer bm.globalMu.RUnlock() // 解锁

	// 获取全局属性 map 快照
	old := bm.globalAttrs.Load().(map[string]attribute.Attribute)
	// 获取属性
	attr, ok := old[key]
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
	// 获取全局属性 map 快照
	old := bm.globalAttrs.Load().(map[string]attribute.Attribute)
	// 创建新的 map，并复制旧的 map 数据
	newMap := make(map[string]attribute.Attribute, len(old))
	// 遍历旧的 map，并复制数据到新的 map
	for k, v := range old {
		// 复制数据
		newMap[k] = v
	}
	// 返回新的 map，只读，防止外部修改
	return newMap
}

// GetInheritedAttribute 获取继承属性
// 如果属性不存在，返回 false
// 如果属性存在，返回 true 和属性值
func (bm *AttributeManager) GetInheritedAttribute(key string) (attribute.Value, bool) {
	bm.inheritableMu.RLock()         // 加读锁
	defer bm.inheritableMu.RUnlock() // 解锁

	// 获取继承属性 map 快照
	old := bm.inheritableAttrs.Load().(map[string]attribute.Attribute)
	// 获取属性
	attr, ok := old[key]
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
	// 获取继承属性 map 快照
	old := bm.inheritableAttrs.Load().(map[string]attribute.Attribute)
	// 创建新的 map，并复制旧的 map 数据
	newMap := make(map[string]attribute.Attribute, len(old))
	// 遍历旧的 map，并复制数据到新的 map
	for k, v := range old {
		// 复制数据
		newMap[k] = v
	}
	// 返回新的 map，只读，防止外部修改
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
		old := bm.inheritableAttrs.Load().(map[string]attribute.Attribute)
		current, exists := old[key]
		if !exists {
			return false
		}
		next := make(map[string]attribute.Attribute, len(old))
		for oldKey, oldValue := range old {
			next[oldKey] = oldValue
		}
		current.Value = value
		next[key] = current
		bm.inheritableAttrs.Store(next)
		return true
	case attribute.AttributeTypeGlobal:
		bm.globalMu.Lock()
		defer bm.globalMu.Unlock()
		old := bm.globalAttrs.Load().(map[string]attribute.Attribute)
		current, exists := old[key]
		if !exists {
			return false
		}
		next := make(map[string]attribute.Attribute, len(old))
		for oldKey, oldValue := range old {
			next[oldKey] = oldValue
		}
		current.Value = value
		next[key] = current
		bm.globalAttrs.Store(next)
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
		old := bm.inheritableAttrs.Load().(map[string]attribute.Attribute)
		if _, exists := old[key]; !exists {
			return false
		}
		next := make(map[string]attribute.Attribute, len(old)-1)
		for oldKey, oldValue := range old {
			if oldKey != key {
				next[oldKey] = oldValue
			}
		}
		bm.inheritableAttrs.Store(next)
		return true
	case attribute.AttributeTypeGlobal:
		bm.globalMu.Lock()
		defer bm.globalMu.Unlock()
		old := bm.globalAttrs.Load().(map[string]attribute.Attribute)
		if _, exists := old[key]; !exists {
			return false
		}
		next := make(map[string]attribute.Attribute, len(old)-1)
		for oldKey, oldValue := range old {
			if oldKey != key {
				next[oldKey] = oldValue
			}
		}
		bm.globalAttrs.Store(next)
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
	// 继承属性
	case attribute.AttributeTypeInherited:
		bm.inheritableMu.Lock()         // 加写锁
		defer bm.inheritableMu.Unlock() // 解锁
		// 重置继承属性
		bm.inheritableAttrs.Store(make(map[string]attribute.Attribute))
		return true

	// 全局属性
	case attribute.AttributeTypeGlobal:
		bm.globalMu.Lock()         // 加写锁
		defer bm.globalMu.Unlock() // 解锁
		// 重置全局属性
		bm.globalAttrs.Store(make(map[string]attribute.Attribute))
		return true
	}
	return false
}

// ResetAll 重置属性管理器
func (bm *AttributeManager) ResetAll() {
	bm.inheritableMu.Lock()         // 加写锁
	defer bm.inheritableMu.Unlock() // 解锁
	bm.globalMu.Lock()              // 加写锁
	defer bm.globalMu.Unlock()      // 解锁

	// 重置继承属性
	bm.inheritableAttrs.Store(make(map[string]attribute.Attribute))
	// 重置全局属性
	bm.globalAttrs.Store(make(map[string]attribute.Attribute))
}
