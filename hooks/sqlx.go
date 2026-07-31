//go:build sqlx_hook
// +build sqlx_hook

package hooks

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/baggage"
	"github.com/ClownSketch/tracer/types/operation"
	"github.com/jmoiron/sqlx"
)

// TracedDB 包装 sqlx.DB，添加追踪功能
type TracedDB struct {
	*sqlx.DB
}

// NewTracedDB 创建一个带追踪的 sqlx.DB 包装器
func NewTracedDB(db *sqlx.DB) *TracedDB {
	return &TracedDB{DB: db}
}

// QueryContext 执行查询并返回多行结果
func (tdb *TracedDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	startTime := time.Now()
	rows, err := tdb.DB.QueryContext(ctx, query, args...)
	duration := time.Since(startTime)

	tdb.recordSQLOperation(ctx, query, args, "SELECT", duration, err, 0)
	return rows, err
}

// QueryxContext 执行查询并返回 sqlx.Rows
func (tdb *TracedDB) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	startTime := time.Now()
	rows, err := tdb.DB.QueryxContext(ctx, query, args...)
	duration := time.Since(startTime)

	tdb.recordSQLOperation(ctx, query, args, "SELECT", duration, err, 0)
	return rows, err
}

// QueryRowxContext 执行查询并返回单行结果
func (tdb *TracedDB) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	startTime := time.Now()
	row := tdb.DB.QueryRowxContext(ctx, query, args...)
	duration := time.Since(startTime)

	// QueryRowxContext 不会立即返回错误，需要在 Scan 时检查
	// 这里先记录操作，错误会在 Scan 时捕获
	tdb.recordSQLOperation(ctx, query, args, "SELECT", duration, nil, 0)
	return row
}

// ExecContext 执行 SQL 语句（INSERT, UPDATE, DELETE 等）
func (tdb *TracedDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	startTime := time.Now()
	result, err := tdb.DB.ExecContext(ctx, query, args...)
	duration := time.Since(startTime)

	var rowsAffected int64
	if result != nil && err == nil {
		rowsAffected, _ = result.RowsAffected()
	}

	operation := determineSQLOperation(query)
	tdb.recordSQLOperation(ctx, query, args, operation, duration, err, rowsAffected)
	return result, err
}

// GetContext 执行查询并将结果映射到单个结构体
func (tdb *TracedDB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	startTime := time.Now()
	err := tdb.DB.GetContext(ctx, dest, query, args...)
	duration := time.Since(startTime)

	tdb.recordSQLOperation(ctx, query, args, "SELECT", duration, err, 0)
	return err
}

// SelectContext 执行查询并将结果映射到切片
func (tdb *TracedDB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	startTime := time.Now()
	err := tdb.DB.SelectContext(ctx, dest, query, args...)
	duration := time.Since(startTime)

	tdb.recordSQLOperation(ctx, query, args, "SELECT", duration, err, 0)
	return err
}

// NamedExecContext 执行命名参数的 SQL 语句
func (tdb *TracedDB) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	startTime := time.Now()
	result, err := tdb.DB.NamedExecContext(ctx, query, arg)
	duration := time.Since(startTime)

	var rowsAffected int64
	if result != nil && err == nil {
		rowsAffected, _ = result.RowsAffected()
	}

	operation := determineSQLOperation(query)
	tdb.recordSQLOperation(ctx, query, nil, operation, duration, err, rowsAffected)
	return result, err
}

// NamedQueryContext 执行命名参数的查询
func (tdb *TracedDB) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	startTime := time.Now()
	rows, err := tdb.DB.NamedQueryContext(ctx, query, arg)
	duration := time.Since(startTime)

	tdb.recordSQLOperation(ctx, query, nil, "SELECT", duration, err, 0)
	return rows, err
}

// PreparexContext 准备一个带命名参数的语句
func (tdb *TracedDB) PreparexContext(ctx context.Context, query string) (*TracedStmt, error) {
	stmt, err := tdb.DB.PreparexContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &TracedStmt{Stmt: stmt, query: query}, nil
}

// Beginx 开始一个事务
func (tdb *TracedDB) Beginx() (*TracedTx, error) {
	tx, err := tdb.DB.Beginx()
	if err != nil {
		return nil, err
	}
	return &TracedTx{Tx: tx}, nil
}

// BeginTxx 开始一个带上下文的事务
func (tdb *TracedDB) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*TracedTx, error) {
	tx, err := tdb.DB.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &TracedTx{Tx: tx}, nil
}

// recordSQLOperation 记录 SQL 操作到追踪系统
func (tdb *TracedDB) recordSQLOperation(
	ctx context.Context,
	query string,
	args []interface{},
	operationType string,
	duration time.Duration,
	err error,
	rowsAffected int64,
) {
	span := baggage.GetSpanContext(ctx)

	// 如果 span 不存在或者是 noop span，则不记录
	if span == nil || span.GetSpanName() == "" {
		return
	}

	// 构建完整的 SQL 语句（包含参数）
	sql := buildSQLWithArgs(query, args)

	var errMsg string
	var stack string
	if err != nil {
		errMsg = err.Error()
		stack = getStackTrace()
	}

	// 从查询中提取表名（简单实现，可以通过解析 SQL 改进）
	table := extractTableName(query)

	// 记录 SQL 操作事件
	span.AddEvent("sql.operations", operationType, tracer.BuildSQLOperationEvent(&operation.SQLOperationInfo{
		Timestamp:   time.Now().Format(time.RFC3339),
		Stack:       stack,
		SQL:         sql,
		Table:       table,
		Operation:   operationType,
		Rows:        rowsAffected,
		Message:     errMsg,
		Success:     err == nil,
		CostSeconds: duration.Seconds(),
		Transaction: false, // 单个操作不是事务，事务由 TracedTx 处理
	}))
}

// TracedStmt 包装 sqlx.Stmt，添加追踪功能
type TracedStmt struct {
	*sqlx.Stmt
	query string
}

// ExecContext 执行准备好的语句
func (ts *TracedStmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	startTime := time.Now()
	result, err := ts.Stmt.ExecContext(ctx, args...)
	duration := time.Since(startTime)

	var rowsAffected int64
	if result != nil && err == nil {
		rowsAffected, _ = result.RowsAffected()
	}

	operation := determineSQLOperation(ts.query)
	ts.recordSQLOperation(ctx, ts.query, args, operation, duration, err, rowsAffected)
	return result, err
}

// QueryContext 执行准备好的查询
func (ts *TracedStmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	startTime := time.Now()
	rows, err := ts.Stmt.QueryContext(ctx, args...)
	duration := time.Since(startTime)

	ts.recordSQLOperation(ctx, ts.query, args, "SELECT", duration, err, 0)
	return rows, err
}

// QueryxContext 执行准备好的查询并返回 sqlx.Rows
func (ts *TracedStmt) QueryxContext(ctx context.Context, args ...interface{}) (*sqlx.Rows, error) {
	startTime := time.Now()
	rows, err := ts.Stmt.QueryxContext(ctx, args...)
	duration := time.Since(startTime)

	ts.recordSQLOperation(ctx, ts.query, args, "SELECT", duration, err, 0)
	return rows, err
}

// QueryRowxContext 执行准备好的查询并返回单行结果
func (ts *TracedStmt) QueryRowxContext(ctx context.Context, args ...interface{}) *sqlx.Row {
	startTime := time.Now()
	row := ts.Stmt.QueryRowxContext(ctx, args...)
	duration := time.Since(startTime)

	ts.recordSQLOperation(ctx, ts.query, args, "SELECT", duration, nil, 0)
	return row
}

// GetContext 执行准备好的查询并将结果映射到结构体
func (ts *TracedStmt) GetContext(ctx context.Context, dest interface{}, args ...interface{}) error {
	startTime := time.Now()
	err := ts.Stmt.GetContext(ctx, dest, args...)
	duration := time.Since(startTime)

	ts.recordSQLOperation(ctx, ts.query, args, "SELECT", duration, err, 0)
	return err
}

// SelectContext 执行准备好的查询并将结果映射到切片
func (ts *TracedStmt) SelectContext(ctx context.Context, dest interface{}, args ...interface{}) error {
	startTime := time.Now()
	err := ts.Stmt.SelectContext(ctx, dest, args...)
	duration := time.Since(startTime)

	ts.recordSQLOperation(ctx, ts.query, args, "SELECT", duration, err, 0)
	return err
}

// recordSQLOperation 记录 SQL 操作（TracedStmt 使用）
func (ts *TracedStmt) recordSQLOperation(
	ctx context.Context,
	query string,
	args []interface{},
	operationType string,
	duration time.Duration,
	err error,
	rowsAffected int64,
) {
	span := baggage.GetSpanContext(ctx)

	if span == nil || span.GetSpanName() == "" {
		return
	}

	sql := buildSQLWithArgs(query, args)

	var errMsg string
	var stack string
	if err != nil {
		errMsg = err.Error()
		stack = getStackTrace()
	}

	table := extractTableName(query)

	span.AddEvent("sql.operations", operationType, tracer.BuildSQLOperationEvent(&operation.SQLOperationInfo{
		Timestamp:   time.Now().Format(time.RFC3339),
		Stack:       stack,
		SQL:         sql,
		Table:       table,
		Operation:   operationType,
		Rows:        rowsAffected,
		Message:     errMsg,
		Success:     err == nil,
		CostSeconds: duration.Seconds(),
		Transaction: false,
	}))
}

// TracedTx 包装 sqlx.Tx，添加追踪功能
type TracedTx struct {
	*sqlx.Tx
}

// ExecContext 在事务中执行 SQL 语句
func (ttx *TracedTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	startTime := time.Now()
	result, err := ttx.Tx.ExecContext(ctx, query, args...)
	duration := time.Since(startTime)

	var rowsAffected int64
	if result != nil && err == nil {
		rowsAffected, _ = result.RowsAffected()
	}

	operation := determineSQLOperation(query)
	ttx.recordSQLOperation(ctx, query, args, operation, duration, err, rowsAffected, true)
	return result, err
}

// QueryContext 在事务中执行查询
func (ttx *TracedTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	startTime := time.Now()
	rows, err := ttx.Tx.QueryContext(ctx, query, args...)
	duration := time.Since(startTime)

	ttx.recordSQLOperation(ctx, query, args, "SELECT", duration, err, 0, true)
	return rows, err
}

// QueryxContext 在事务中执行查询并返回 sqlx.Rows
func (ttx *TracedTx) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	startTime := time.Now()
	rows, err := ttx.Tx.QueryxContext(ctx, query, args...)
	duration := time.Since(startTime)

	ttx.recordSQLOperation(ctx, query, args, "SELECT", duration, err, 0, true)
	return rows, err
}

// QueryRowxContext 在事务中执行查询并返回单行结果
func (ttx *TracedTx) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	startTime := time.Now()
	row := ttx.Tx.QueryRowxContext(ctx, query, args...)
	duration := time.Since(startTime)

	ttx.recordSQLOperation(ctx, query, args, "SELECT", duration, nil, 0, true)
	return row
}

// GetContext 在事务中执行查询并将结果映射到结构体
func (ttx *TracedTx) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	startTime := time.Now()
	err := ttx.Tx.GetContext(ctx, dest, query, args...)
	duration := time.Since(startTime)

	ttx.recordSQLOperation(ctx, query, args, "SELECT", duration, err, 0, true)
	return err
}

// SelectContext 在事务中执行查询并将结果映射到切片
func (ttx *TracedTx) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	startTime := time.Now()
	err := ttx.Tx.SelectContext(ctx, dest, query, args...)
	duration := time.Since(startTime)

	ttx.recordSQLOperation(ctx, query, args, "SELECT", duration, err, 0, true)
	return err
}

// NamedExecContext 在事务中执行命名参数的 SQL 语句
func (ttx *TracedTx) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	startTime := time.Now()
	result, err := ttx.Tx.NamedExecContext(ctx, query, arg)
	duration := time.Since(startTime)

	var rowsAffected int64
	if result != nil && err == nil {
		rowsAffected, _ = result.RowsAffected()
	}

	operation := determineSQLOperation(query)
	ttx.recordSQLOperation(ctx, query, nil, operation, duration, err, rowsAffected, true)
	return result, err
}

// NamedQueryContext 在事务中执行命名参数的查询
func (ttx *TracedTx) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	startTime := time.Now()
	rows, err := sqlx.NamedQueryContext(ctx, ttx.Tx, query, arg)
	duration := time.Since(startTime)

	ttx.recordSQLOperation(ctx, query, nil, "SELECT", duration, err, 0, true)
	return rows, err
}

// PreparexContext 在事务中准备一个带命名参数的语句
func (ttx *TracedTx) PreparexContext(ctx context.Context, query string) (*TracedStmt, error) {
	stmt, err := ttx.Tx.PreparexContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &TracedStmt{Stmt: stmt, query: query}, nil
}

// recordSQLOperation 记录 SQL 操作（TracedTx 使用）
func (ttx *TracedTx) recordSQLOperation(
	ctx context.Context,
	query string,
	args []interface{},
	operationType string,
	duration time.Duration,
	err error,
	rowsAffected int64,
	isTransaction bool,
) {
	span := baggage.GetSpanContext(ctx)

	if span == nil || span.GetSpanName() == "" {
		return
	}

	sql := buildSQLWithArgs(query, args)

	var errMsg string
	var stack string
	if err != nil {
		errMsg = err.Error()
		stack = getStackTrace()
	}

	table := extractTableName(query)

	span.AddEvent("sql.operations", operationType, tracer.BuildSQLOperationEvent(&operation.SQLOperationInfo{
		Timestamp:   time.Now().Format(time.RFC3339),
		Stack:       stack,
		SQL:         sql,
		Table:       table,
		Operation:   operationType,
		Rows:        rowsAffected,
		Message:     errMsg,
		Success:     err == nil,
		CostSeconds: duration.Seconds(),
		Transaction: isTransaction,
	}))
}

// determineSQLOperation 从 SQL 语句中确定操作类型
func determineSQLOperation(query string) string {
	queryUpper := strings.ToUpper(strings.TrimSpace(query))
	switch {
	case strings.HasPrefix(queryUpper, "SELECT"):
		return "SELECT"
	case strings.HasPrefix(queryUpper, "INSERT"):
		return "INSERT"
	case strings.HasPrefix(queryUpper, "UPDATE"):
		return "UPDATE"
	case strings.HasPrefix(queryUpper, "DELETE"):
		return "DELETE"
	case strings.HasPrefix(queryUpper, "REPLACE"):
		return "REPLACE"
	default:
		return "OTHER"
	}
}

// buildSQLWithArgs 构建包含参数的 SQL 语句（用于日志记录）
// 注意：这是一个简化的实现，主要用于追踪和日志记录
// 对于复杂的 SQL（如包含字符串中的 ? 等），可能需要更复杂的解析
func buildSQLWithArgs(query string, args []interface{}) string {
	if len(args) == 0 {
		return query
	}

	// 检查是否使用命名参数（:name 格式）
	if strings.Contains(query, ":") && !strings.Contains(query, "?") {
		// 命名参数由 sqlx 内部处理，这里只返回原始查询
		return query
	}

	// 处理位置参数（? 或 $1, $2 格式）
	sql := query
	argIndex := 0

	// 处理 $1, $2, $3... 格式（PostgreSQL）
	if strings.Contains(query, "$") {
		for argIndex < len(args) {
			placeholder := fmt.Sprintf("$%d", argIndex+1)
			if strings.Contains(sql, placeholder) {
				argStr := formatArg(args[argIndex])
				sql = strings.Replace(sql, placeholder, argStr, 1)
				argIndex++
			} else {
				break
			}
		}
	} else {
		// 处理 ? 格式（MySQL, SQLite 等）
		for argIndex < len(args) {
			if idx := strings.Index(sql, "?"); idx >= 0 {
				argStr := formatArg(args[argIndex])
				sql = sql[:idx] + argStr + sql[idx+1:]
				argIndex++
			} else {
				break
			}
		}
	}

	return sql
}

// formatArg 格式化参数为字符串
func formatArg(arg interface{}) string {
	if arg == nil {
		return "NULL"
	}

	switch v := arg.(type) {
	case string:
		return "'" + v + "'"
	case int:
		return fmt.Sprintf("%d", v)
	case int8:
		return fmt.Sprintf("%d", v)
	case int16:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case uint:
		return fmt.Sprintf("%d", v)
	case uint8:
		return fmt.Sprintf("%d", v)
	case uint16:
		return fmt.Sprintf("%d", v)
	case uint32:
		return fmt.Sprintf("%d", v)
	case uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%f", v)
	case float64:
		return fmt.Sprintf("%f", v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		// 对于其他类型，使用 fmt.Sprintf 转换为字符串
		return fmt.Sprintf("'%v'", v)
	}
}

// extractTableName 从 SQL 语句中提取表名（简单实现）
func extractTableName(query string) string {
	queryUpper := strings.ToUpper(strings.TrimSpace(query))

	// SELECT ... FROM table
	if strings.Contains(queryUpper, "FROM") {
		parts := strings.Split(queryUpper, "FROM")
		if len(parts) > 1 {
			tablePart := strings.TrimSpace(parts[1])
			// 移除可能的 WHERE, JOIN, LIMIT 等
			tablePart = strings.Fields(tablePart)[0]
			return strings.Trim(tablePart, "`\"'")
		}
	}

	// INSERT INTO table
	if strings.Contains(queryUpper, "INSERT INTO") {
		parts := strings.Split(queryUpper, "INSERT INTO")
		if len(parts) > 1 {
			tablePart := strings.TrimSpace(parts[1])
			tablePart = strings.Fields(tablePart)[0]
			return strings.Trim(tablePart, "`\"'")
		}
	}

	// UPDATE table
	if strings.Contains(queryUpper, "UPDATE") {
		parts := strings.Split(queryUpper, "UPDATE")
		if len(parts) > 1 {
			tablePart := strings.TrimSpace(parts[1])
			tablePart = strings.Fields(tablePart)[0]
			return strings.Trim(tablePart, "`\"'")
		}
	}

	// DELETE FROM table
	if strings.Contains(queryUpper, "DELETE FROM") {
		parts := strings.Split(queryUpper, "DELETE FROM")
		if len(parts) > 1 {
			tablePart := strings.TrimSpace(parts[1])
			tablePart = strings.Fields(tablePart)[0]
			return strings.Trim(tablePart, "`\"'")
		}
	}

	return ""
}

// getStackTrace 获取堆栈信息
func getStackTrace() string {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	if n > 0 {
		return string(buf[:n])
	}
	return ""
}
