//go:build !gorm_hook
// +build !gorm_hook

package hooks

import "gorm.io/gorm"

// GormHookPlugin 是一个占位符，当未开启 gorm_hook 构建标签时不会执行任何逻辑。
type GormHookPlugin struct{}

// Name 返回默认插件名称。
func (h *GormHookPlugin) Name() string {
	return "tracePlugin"
}

// Initialize 在未启用 gorm_hook 时不执行任何操作。
// 注意：方法签名必须与 gorm.go 中的实现保持一致，以满足 GORM 的 Plugin 接口要求。
func (h *GormHookPlugin) Initialize(_ *gorm.DB) error {
	return nil
}
