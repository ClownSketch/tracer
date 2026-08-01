package providers

import (
	"testing"
	"time"
)

// ExampleConsoleExporterConfig 展示如何使用 ConsoleExporterConfig
func ExampleConsoleExporterConfig() {
	// 创建 Console 导出器配置（类型安全，无需断言）
	config := ConsoleExporterConfig{
		Writer:      "logs/trace.log", // 可以是文件路径
		PrettyPrint: true,
		UseJSON:     false,
	}

	// 使用泛型 API 创建导出器（类型安全）
	exporter, err := CreateExporterFromOption(config)
	if err != nil {
		panic(err)
	}
	_ = exporter
}

// ExampleFileExporterConfig 展示如何使用 FileExporterConfig
func ExampleFileExporterConfig() {
	// 创建 File 导出器配置（类型安全，无需断言）
	config := FileExporterConfig{
		FilePath:        "logs/trace.log",
		MaxFileSize:     100 * 1024 * 1024, // 100MB
		RotateInterval:  24 * time.Hour,    // 24小时轮转
		MaxBackups:      10,
		AsyncBufferSize: 1000,
	}

	// 使用泛型 API 创建导出器（类型安全）
	exporter, err := CreateExporterFromOption(config)
	if err != nil {
		panic(err)
	}
	_ = exporter
}

// ExampleExporterConfig 展示如何使用 ExporterConfig 泛型类型
func ExampleExporterConfig() {
	// 方式1：先创建配置，然后创建 ExporterConfig
	config := ConsoleExporterConfig{
		PrettyPrint: true,
		UseJSON:     true,
	}
	exporterConfig, err := NewExporterConfig(config)
	if err != nil {
		panic(err)
	}

	// 使用 ExporterConfig 创建导出器
	exporter, err := CreateExporter(exporterConfig)
	if err != nil {
		panic(err)
	}
	_ = exporter
}

// ExampleMongoDBExporterConfig 展示如何使用 MongoDBExporterConfig
func ExampleMongoDBExporterConfig() {
	// 创建 MongoDB 导出器配置（类型安全，无需断言）
	config := MongoDBExporterConfig{
		URI:        "mongodb://localhost:27017",
		Database:   "tracer",
		Collection: "spans",
		Timeout:    10 * time.Second,
	}

	// 使用泛型 API 创建导出器（类型安全）
	exporter, err := CreateExporterFromOption(config)
	if err != nil {
		panic(err)
	}
	_ = exporter
}

// TestTypeSafety 测试类型安全性
func TestTypeSafety(t *testing.T) {
	// 测试：编译器会确保类型匹配
	config := ConsoleExporterConfig{
		PrettyPrint: true,
	}

	// 这是类型安全的，编译器会检查类型
	_, err := CreateExporterFromOption(config)
	if err != nil {
		t.Fatalf("创建导出器失败: %v", err)
	}

	// 如果尝试使用错误的配置类型，编译器会报错
	// fileConfig := FileExporterConfig{FilePath: "test.log"}
	// _, err = CreateExporterFromOption(fileConfig) // 这仍然可以工作，因为 FileExporterConfig 也实现了 ExporterOption
	// 但如果尝试将 ConsoleExporterConfig 传递给期望 FileExporterConfig 的函数，编译器会报错
}
