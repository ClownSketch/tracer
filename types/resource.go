package types

// ResourceInfo 表示服务/实例的静态信息
type ResourceInfo struct {
	ServiceName string         `json:"service_name"` // 服务名称
	Host        string         `json:"host"`         // 主机名
	Attributes  map[string]any `json:"attributes"`   // 资源属性，可用于存储自定义数据
}

// ResourceMetrics 表示资源使用情况
type ResourceMetrics struct {
	CPUUsage    float64 `json:"cpu_usage"`    // CPU使用率，单位：百分比
	MemoryUsage float64 `json:"memory_usage"` // 内存使用率，单位：MB
	DiskUsage   float64 `json:"disk_usage"`   // 磁盘使用率，单位：MB
	NetworkIO   float64 `json:"network_io"`   // 网络IO，单位：MB
}
