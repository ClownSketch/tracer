package providers

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInitTracer_EnablesDefaultFallbackDir(t *testing.T) {
	tempDir := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("获取当前工作目录失败: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("切换工作目录失败: %v", err)
	}
	defer func() {
		_ = os.Chdir(oldWD)
	}()

	provider, err := InitTracer(TracerConfig{
		ServiceName:    "test-service",
		SampleRate:     1.0,
		ExporterType:   ExporterTypeFile,
		LogFile:        "./storage/log/traces.log",
		BatchSize:      10,
		BatchInterval:  10 * time.Millisecond,
		FileMaxBackups: 1,
	})
	if err != nil {
		t.Fatalf("初始化 Tracer 失败: %v", err)
	}
	defer provider.Shutdown(context.Background())

	fallbackDir := filepath.Join(tempDir, "storage", "fallback")
	info, err := os.Stat(fallbackDir)
	if err != nil {
		t.Fatalf("期望默认 fallback 目录被自动创建，实际错误: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("期望 %s 是目录", fallbackDir)
	}
}

func TestInitTracerReturnsFallbackInitializationError(t *testing.T) {
	tempDir := t.TempDir()
	blockingFile := filepath.Join(tempDir, "not-a-directory")
	if err := os.WriteFile(blockingFile, []byte("block"), 0o600); err != nil {
		t.Fatalf("创建阻断文件失败: %v", err)
	}

	provider, err := InitTracer(TracerConfig{
		ServiceName:    "fallback-init-error",
		SampleRate:     1.0,
		ExporterType:   ExporterTypeFile,
		LogFile:        filepath.Join(tempDir, "traces.log"),
		FallbackDir:    filepath.Join(blockingFile, "fallback"),
		BatchSize:      10,
		BatchInterval:  10 * time.Millisecond,
		FileMaxBackups: 1,
	})
	if err == nil {
		t.Fatal("fallback 目录初始化失败时应返回错误")
	}
	if provider != nil {
		t.Fatal("fallback 初始化失败后不应返回 provider")
	}
}
