//go:build !sqlx_hook
// +build !sqlx_hook

package hooks

import "github.com/jmoiron/sqlx"

// TracedDB 是一个占位符，当未开启 sqlx_hook 构建标签时不会执行任何追踪逻辑。
// 注意：方法签名必须与 sqlx.go 中的实现保持一致，以满足 sqlx 接口要求。
type TracedDB struct {
	*sqlx.DB
}

// NewTracedDB 在未启用 sqlx_hook 时直接返回原始的 sqlx.DB 包装。
func NewTracedDB(db *sqlx.DB) *TracedDB {
	return &TracedDB{DB: db}
}

// TracedStmt 是一个占位符，当未开启 sqlx_hook 构建标签时不会执行任何追踪逻辑。
type TracedStmt struct {
	*sqlx.Stmt
}

// TracedTx 是一个占位符，当未开启 sqlx_hook 构建标签时不会执行任何追踪逻辑。
type TracedTx struct {
	*sqlx.Tx
}
