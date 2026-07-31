//go:build gorm_hook
// +build gorm_hook

package hooks

import (
	"strings"
	"time"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/baggage"
	hooktypes "github.com/ClownSketch/tracer/hooks/type"
	"github.com/ClownSketch/tracer/types/operation"
	"gorm.io/gorm"
	gormutils "gorm.io/gorm/utils"
)

// GormHookPlugin GORM追踪插件
type GormHookPlugin struct {
}

func (h *GormHookPlugin) Name() string {
	return "tracePlugin"
}

// Initialize 初始化插件
func (h *GormHookPlugin) Initialize(db *gorm.DB) error {
	// 注册创建操作的回调
	if err := db.Callback().Create().Before("gorm:create").Register(hooktypes.CallBackBeforeName, beforeOperation); err != nil {
		return err
	}
	if err := db.Callback().Create().After("gorm:create").Register(hooktypes.CallBackAfterName, afterOperation); err != nil {
		return err
	}

	// 注册查询操作的回调
	if err := db.Callback().Query().Before("gorm:query").Register(hooktypes.CallBackBeforeName, beforeOperation); err != nil {
		return err
	}
	if err := db.Callback().Query().After("gorm:query").Register(hooktypes.CallBackAfterName, afterOperation); err != nil {
		return err
	}

	// 注册更新操作的回调
	if err := db.Callback().Update().Before("gorm:update").Register(hooktypes.CallBackBeforeName, beforeOperation); err != nil {
		return err
	}
	if err := db.Callback().Update().After("gorm:update").Register(hooktypes.CallBackAfterName, afterOperation); err != nil {
		return err
	}

	// 注册删除操作的回调
	if err := db.Callback().Delete().Before("gorm:delete").Register(hooktypes.CallBackBeforeName, beforeOperation); err != nil {
		return err
	}
	if err := db.Callback().Delete().After("gorm:delete").Register(hooktypes.CallBackAfterName, afterOperation); err != nil {
		return err
	}

	// 注册原生SQL操作的回调
	if err := db.Callback().Raw().Before("gorm:raw").Register(hooktypes.CallBackBeforeName, beforeOperation); err != nil {
		return err
	}
	if err := db.Callback().Raw().After("gorm:raw").Register(hooktypes.CallBackAfterName, afterOperation); err != nil {
		return err
	}

	// 注册单行查询操作的回调（如 Scan 场景）
	if err := db.Callback().Row().Before("gorm:row").Register(hooktypes.CallBackBeforeName, beforeOperation); err != nil {
		return err
	}
	if err := db.Callback().Row().After("gorm:row").Register(hooktypes.CallBackAfterName, afterOperation); err != nil {
		return err
	}

	return nil
}

// 在操作执行前调用
func beforeOperation(db *gorm.DB) {
	// 设置操作开始时间
	db.InstanceSet(hooktypes.GormStartTime, time.Now())
}

// 在操作执行后调用
func afterOperation(db *gorm.DB) {

	ctx := db.Statement.Context
	span := baggage.GetSpanContext(ctx)

	// 如果 span 不存在或者是 noop span，则不记录
	if span == nil || span.GetSpanName() == "" {
		return
	}

	_ts, isExist := db.InstanceGet(hooktypes.GormStartTime)
	if !isExist {
		return
	}

	ts, ok := _ts.(time.Time)
	if !ok {
		return
	}

	sql := db.Dialector.Explain(db.Statement.SQL.String(), db.Statement.Vars...)

	var errMsg string
	var stack string
	if db.Error != nil {
		errMsg = db.Error.Error()
		stack = gormutils.FileWithLineNum() // 使用 gorm 的 utils 获取堆栈信息
	}

	// 更新span事件，包含完整的SQL操作历史
	span.AddEvent("sql.operations", determineOperation(db), tracer.BuildSQLOperationEvent(&operation.SQLOperationInfo{
		Timestamp:   time.Now().Format(time.RFC3339),                   // 执行时间 (ISO 8601 格式)，如2021-01-01T00:00:00.000Z
		Stack:       stack,                                             // 堆栈信息
		SQL:         sql,                                               // SQL 语句，这里记录的是完整的SQL语句，包括参数
		Table:       db.Statement.Table,                                // 表名
		Operation:   determineOperation(db),                            // 操作类型，如select、insert、update、delete
		Rows:        db.Statement.RowsAffected,                         // 返回的行数
		Message:     errMsg,                                            // 错误消息，用于记录错误信息
		Success:     db.Error == nil,                                   // 是否成功
		CostSeconds: time.Since(ts).Seconds(),                          // 执行时间(单位秒)
		Transaction: db.Statement.ConnPool != db.Statement.DB.ConnPool, // 是否是事务
	}))
}

// 获取操作类型
func determineOperation(db *gorm.DB) string {
	sql := db.Statement.SQL.String()
	switch {
	case len(sql) >= 6 && strings.HasPrefix(strings.ToUpper(sql), "SELECT"):
		return "SELECT"
	case len(sql) >= 6 && strings.HasPrefix(strings.ToUpper(sql), "INSERT"):
		return "INSERT"
	case len(sql) >= 6 && strings.HasPrefix(strings.ToUpper(sql), "UPDATE"):
		return "UPDATE"
	case len(sql) >= 6 && strings.HasPrefix(strings.ToUpper(sql), "DELETE"):
		return "DELETE"
	default:
		return "OTHER"
	}
}
