package metrics

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ClownSketch/tracer/trace"
)

// PrometheusMetrics Prometheus指标收集器
// 从Span中提取metrics并暴露HTTP端点供Prometheus抓取
// 性能优化：
//   - 使用原子操作保证并发安全
//   - 异步更新metrics，不阻塞Span处理
//   - 使用sync.Map减少锁竞争
type PrometheusMetrics struct {
	// HTTP服务器配置
	server     *http.Server
	serverAddr string // 服务器地址，如 ":9090"
	listener   net.Listener

	// 指标数据（使用原子操作保证并发安全）
	spanCount      int64 // Span总数
	spanDuration   int64 // Span总持续时间（纳秒）
	spanErrorCount int64 // Span错误总数
	spanDropped    int64 // 丢弃的Span总数

	// 按服务名称分组的指标（使用sync.Map减少锁竞争）
	serviceMetrics sync.Map // map[string]*serviceMetric

	// 按操作名称分组的指标
	operationMetrics sync.Map // map[string]*operationMetric
	maxSeries        int64
	serviceSeries    int64
	operationSeries  int64
	seriesDropped    int64

	// 控制
	mu       sync.RWMutex
	seriesMu sync.Mutex
	startErr error
	shutdown chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// serviceMetric 服务级别的指标
type serviceMetric struct {
	Count      int64 // Span总数
	Duration   int64 // 总持续时间（纳秒）
	ErrorCount int64 // 错误总数
}

// operationMetric 操作级别的指标
type operationMetric struct {
	Count      int64 // Span总数
	Duration   int64 // 总持续时间（纳秒）
	ErrorCount int64 // 错误总数
}

// PrometheusMetricsOption Prometheus指标收集器选项
type PrometheusMetricsOption func(*PrometheusMetrics)

// WithPrometheusAddr 设置HTTP服务器地址
func WithPrometheusAddr(addr string) PrometheusMetricsOption {
	return func(m *PrometheusMetrics) {
		m.serverAddr = addr
	}
}

// WithPrometheusMaxSeries 设置服务和操作指标各自允许的最大序列数。
func WithPrometheusMaxSeries(size int) PrometheusMetricsOption {
	return func(m *PrometheusMetrics) {
		if size > 0 {
			m.maxSeries = int64(size)
		}
	}
}

// NewPrometheusMetrics 创建Prometheus指标收集器
func NewPrometheusMetrics(opts ...PrometheusMetricsOption) *PrometheusMetrics {
	metrics, err := NewPrometheusMetricsE(opts...)
	if err == nil {
		return metrics
	}

	metrics = newPrometheusMetrics(opts...)
	metrics.startErr = err
	return metrics
}

// NewPrometheusMetricsE 创建指标收集器并同步返回端口监听错误。
func NewPrometheusMetricsE(opts ...PrometheusMetricsOption) (*PrometheusMetrics, error) {
	metrics := newPrometheusMetrics(opts...)
	listener, err := net.Listen("tcp", metrics.serverAddr)
	if err != nil {
		return nil, err
	}
	metrics.listener = listener

	metrics.wg.Add(1)
	go func() {
		defer metrics.wg.Done()
		metrics.startServer(listener)
	}()
	return metrics, nil
}

// newPrometheusMetrics 构建尚未启动监听的指标收集器。
func newPrometheusMetrics(opts ...PrometheusMetricsOption) *PrometheusMetrics {
	m := &PrometheusMetrics{
		serverAddr: ":9090",
		maxSeries:  1000,
		shutdown:   make(chan struct{}),
	}

	// 应用选项
	for _, opt := range opts {
		opt(m)
	}

	// 创建HTTP服务器
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", m.handleMetrics)
	mux.HandleFunc("/health", m.handleHealth)

	m.server = &http.Server{
		Addr:    m.serverAddr,
		Handler: mux,
	}
	return m
}

// RecordSpan 记录Span指标
func (m *PrometheusMetrics) RecordSpan(span trace.SpanSnapshot) {
	if span == nil {
		return
	}

	// 更新全局指标
	atomic.AddInt64(&m.spanCount, 1)
	duration := span.GetEndTime().Sub(span.GetStartTime()).Nanoseconds()
	atomic.AddInt64(&m.spanDuration, duration)

	// 检查是否有错误
	hasError := false
	if errDetail := span.GetErrorDetail(); errDetail != nil {
		hasError = true
		atomic.AddInt64(&m.spanErrorCount, 1)
	}

	// 获取服务名称
	serviceName := "unknown"
	if resource := span.GetResource(); resource != nil && resource.ServiceName != "" {
		serviceName = resource.ServiceName
	}

	// 更新服务级别指标
	m.updateServiceMetrics(serviceName, duration, hasError)

	// 更新操作级别指标
	operationName := span.GetSpanName()
	m.updateOperationMetrics(operationName, duration, hasError)
}

// RecordDropped 记录丢弃的Span
func (m *PrometheusMetrics) RecordDropped() {
	atomic.AddInt64(&m.spanDropped, 1)
}

// updateServiceMetrics 更新服务级别指标
func (m *PrometheusMetrics) updateServiceMetrics(serviceName string, duration int64, hasError bool) {
	value, ok := m.serviceMetrics.Load(serviceName)
	if !ok {
		m.seriesMu.Lock()
		value, ok = m.serviceMetrics.Load(serviceName)
		if !ok {
			if atomic.LoadInt64(&m.serviceSeries) >= m.maxSeries {
				atomic.AddInt64(&m.seriesDropped, 1)
				m.seriesMu.Unlock()
				return
			}
			value = &serviceMetric{}
			m.serviceMetrics.Store(serviceName, value)
			atomic.AddInt64(&m.serviceSeries, 1)
		}
		m.seriesMu.Unlock()
	}
	metric := value.(*serviceMetric)

	atomic.AddInt64(&metric.Count, 1)
	atomic.AddInt64(&metric.Duration, duration)
	if hasError {
		atomic.AddInt64(&metric.ErrorCount, 1)
	}
}

// updateOperationMetrics 更新操作级别指标
func (m *PrometheusMetrics) updateOperationMetrics(operationName string, duration int64, hasError bool) {
	value, ok := m.operationMetrics.Load(operationName)
	if !ok {
		m.seriesMu.Lock()
		value, ok = m.operationMetrics.Load(operationName)
		if !ok {
			if atomic.LoadInt64(&m.operationSeries) >= m.maxSeries {
				atomic.AddInt64(&m.seriesDropped, 1)
				m.seriesMu.Unlock()
				return
			}
			value = &operationMetric{}
			m.operationMetrics.Store(operationName, value)
			atomic.AddInt64(&m.operationSeries, 1)
		}
		m.seriesMu.Unlock()
	}
	metric := value.(*operationMetric)

	atomic.AddInt64(&metric.Count, 1)
	atomic.AddInt64(&metric.Duration, duration)
	if hasError {
		atomic.AddInt64(&metric.ErrorCount, 1)
	}
}

// startServer 启动HTTP服务器
func (m *PrometheusMetrics) startServer(listener net.Listener) {
	defer func() {
		if err := recover(); err != nil {
			// 恢复panic，不输出日志
			_ = err
		}
	}()

	if err := m.server.Serve(listener); err != nil && err != http.ErrServerClosed {
		m.mu.Lock()
		m.startErr = err
		m.mu.Unlock()
	}
}

// GetLastError 返回指标服务最近一次启动错误。
func (m *PrometheusMetrics) GetLastError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.startErr
}

// handleMetrics 处理/metrics端点
func (m *PrometheusMetrics) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	// 全局指标
	spanCount := atomic.LoadInt64(&m.spanCount)
	spanDuration := atomic.LoadInt64(&m.spanDuration)
	spanErrorCount := atomic.LoadInt64(&m.spanErrorCount)
	spanDropped := atomic.LoadInt64(&m.spanDropped)
	seriesDropped := atomic.LoadInt64(&m.seriesDropped)

	// 计算平均持续时间
	var avgDuration float64
	if spanCount > 0 {
		avgDuration = float64(spanDuration) / float64(spanCount) / 1e9 // 转换为秒
	}

	// 计算错误率
	var errorRate float64
	if spanCount > 0 {
		errorRate = float64(spanErrorCount) / float64(spanCount)
	}

	// 输出全局指标
	fmt.Fprintf(w, "# HELP tracer_spans_total Total number of spans\n")
	fmt.Fprintf(w, "# TYPE tracer_spans_total counter\n")
	fmt.Fprintf(w, "tracer_spans_total %d\n", spanCount)

	fmt.Fprintf(w, "# HELP tracer_spans_duration_seconds_total Total duration of all spans in seconds\n")
	fmt.Fprintf(w, "# TYPE tracer_spans_duration_seconds_total counter\n")
	fmt.Fprintf(w, "tracer_spans_duration_seconds_total %.6f\n", float64(spanDuration)/1e9)

	fmt.Fprintf(w, "# HELP tracer_spans_duration_seconds_avg Average duration of spans in seconds\n")
	fmt.Fprintf(w, "# TYPE tracer_spans_duration_seconds_avg gauge\n")
	fmt.Fprintf(w, "tracer_spans_duration_seconds_avg %.6f\n", avgDuration)

	fmt.Fprintf(w, "# HELP tracer_spans_errors_total Total number of error spans\n")
	fmt.Fprintf(w, "# TYPE tracer_spans_errors_total counter\n")
	fmt.Fprintf(w, "tracer_spans_errors_total %d\n", spanErrorCount)

	fmt.Fprintf(w, "# HELP tracer_spans_error_rate Error rate of spans\n")
	fmt.Fprintf(w, "# TYPE tracer_spans_error_rate gauge\n")
	fmt.Fprintf(w, "tracer_spans_error_rate %.6f\n", errorRate)

	fmt.Fprintf(w, "# HELP tracer_spans_dropped_total Total number of dropped spans\n")
	fmt.Fprintf(w, "# TYPE tracer_spans_dropped_total counter\n")
	fmt.Fprintf(w, "tracer_spans_dropped_total %d\n", spanDropped)

	fmt.Fprintln(w, "# HELP tracer_metric_series_dropped_total Total number of metric series rejected by the cardinality limit")
	fmt.Fprintln(w, "# TYPE tracer_metric_series_dropped_total counter")
	fmt.Fprintf(w, "tracer_metric_series_dropped_total %d\n", seriesDropped)

	fmt.Fprintln(w, "# HELP tracer_service_spans_total Total number of spans per service")
	fmt.Fprintln(w, "# TYPE tracer_service_spans_total counter")
	fmt.Fprintln(w, "# HELP tracer_service_spans_duration_seconds_avg Average duration of spans per service in seconds")
	fmt.Fprintln(w, "# TYPE tracer_service_spans_duration_seconds_avg gauge")
	fmt.Fprintln(w, "# HELP tracer_service_spans_error_rate Error rate of spans per service")
	fmt.Fprintln(w, "# TYPE tracer_service_spans_error_rate gauge")

	// 服务级别指标
	m.serviceMetrics.Range(func(key, value any) bool {
		serviceName := escapePrometheusLabel(key.(string))
		metric := value.(*serviceMetric)

		count := atomic.LoadInt64(&metric.Count)
		duration := atomic.LoadInt64(&metric.Duration)
		errorCount := atomic.LoadInt64(&metric.ErrorCount)

		var avgDur float64
		if count > 0 {
			avgDur = float64(duration) / float64(count) / 1e9
		}

		var errRate float64
		if count > 0 {
			errRate = float64(errorCount) / float64(count)
		}

		fmt.Fprintf(w, "tracer_service_spans_total{service=\"%s\"} %d\n", serviceName, count)

		fmt.Fprintf(w, "tracer_service_spans_duration_seconds_avg{service=\"%s\"} %.6f\n", serviceName, avgDur)

		fmt.Fprintf(w, "tracer_service_spans_error_rate{service=\"%s\"} %.6f\n", serviceName, errRate)

		return true
	})

	fmt.Fprintln(w, "# HELP tracer_operation_spans_total Total number of spans per operation")
	fmt.Fprintln(w, "# TYPE tracer_operation_spans_total counter")
	fmt.Fprintln(w, "# HELP tracer_operation_spans_duration_seconds_avg Average duration of spans per operation in seconds")
	fmt.Fprintln(w, "# TYPE tracer_operation_spans_duration_seconds_avg gauge")
	fmt.Fprintln(w, "# HELP tracer_operation_spans_error_rate Error rate of spans per operation")
	fmt.Fprintln(w, "# TYPE tracer_operation_spans_error_rate gauge")

	// 操作级别指标
	m.operationMetrics.Range(func(key, value any) bool {
		operationName := escapePrometheusLabel(key.(string))
		metric := value.(*operationMetric)

		count := atomic.LoadInt64(&metric.Count)
		duration := atomic.LoadInt64(&metric.Duration)
		errorCount := atomic.LoadInt64(&metric.ErrorCount)

		var avgDur float64
		if count > 0 {
			avgDur = float64(duration) / float64(count) / 1e9
		}

		var errRate float64
		if count > 0 {
			errRate = float64(errorCount) / float64(count)
		}

		fmt.Fprintf(w, "tracer_operation_spans_total{operation=\"%s\"} %d\n", operationName, count)

		fmt.Fprintf(w, "tracer_operation_spans_duration_seconds_avg{operation=\"%s\"} %.6f\n", operationName, avgDur)

		fmt.Fprintf(w, "tracer_operation_spans_error_rate{operation=\"%s\"} %.6f\n", operationName, errRate)

		return true
	})
}

// escapePrometheusLabel 转义 Prometheus 文本格式中的标签值。
func escapePrometheusLabel(value string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		"\n", `\n`,
		`"`, `\"`,
	)
	return replacer.Replace(value)
}

// handleHealth 处理/health端点
func (m *PrometheusMetrics) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// Shutdown 关闭指标收集器
func (m *PrometheusMetrics) Shutdown(ctx context.Context) error {
	var shutdownErr error
	m.stopOnce.Do(func() {
		close(m.shutdown)

		// 关闭HTTP服务器
		if m.server != nil {
			if err := m.server.Shutdown(ctx); err != nil {
				shutdownErr = err
			}
		}

		// 等待goroutine完成
		done := make(chan struct{})
		go func() {
			defer func() {
				if err := recover(); err != nil {
					// 恢复panic，不输出日志
					_ = err
				}
			}()
			m.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// goroutine已完成
		case <-ctx.Done():
			if shutdownErr == nil {
				shutdownErr = ctx.Err()
			}
		}
	})

	return shutdownErr
}
