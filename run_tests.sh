#!/usr/bin/env bash
# Tracer 测试脚本：执行测试、生成覆盖率与报告；日志写入 log/ 目录

set -euo pipefail

# ---------- 工作目录与 Go 版本 ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
EXPECTED_GO_VERSION="go$(tr -d '[:space:]' < "${SCRIPT_DIR}/.go-version")"

# ---------- goenv：非交互 shell 中必须初始化，否则 PATH 仍指向系统 Go ----------
export GOENV_ROOT="${GOENV_ROOT:-$HOME/.goenv}"
if [[ -d "$GOENV_ROOT/bin" ]]; then
  export PATH="$GOENV_ROOT/bin:$PATH"
fi
if command -v goenv >/dev/null 2>&1; then
  export GOENV_SHELL="bash"
  # shellcheck disable=SC2046
  eval "$(goenv init -)"
else
  echo "警告: 未找到 goenv，将使用当前 PATH 中的 go。请确保与 go.mod 一致。" >&2
fi

if ! command -v go >/dev/null 2>&1; then
  echo "错误: 未找到 Go 命令。" >&2
  exit 1
fi

CURRENT_GO_VERSION="$(go env GOVERSION)"
if [[ "$CURRENT_GO_VERSION" != "$EXPECTED_GO_VERSION" ]]; then
  echo "错误: 当前 Go 为 ${CURRENT_GO_VERSION}，项目要求 ${EXPECTED_GO_VERSION}。" >&2
  exit 1
fi

CURRENT_GOROOT="$(go env GOROOT)"
GOROOT_GO_VERSION="$("${CURRENT_GOROOT}/bin/go" env GOVERSION 2>/dev/null || true)"
if [[ "$GOROOT_GO_VERSION" != "$EXPECTED_GO_VERSION" ]]; then
  echo "错误: GOROOT=${CURRENT_GOROOT} 对应 ${GOROOT_GO_VERSION:-未知版本}，项目要求 ${EXPECTED_GO_VERSION}。" >&2
  echo "请清理旧 GOROOT 配置后重新执行测试。" >&2
  exit 1
fi

# ---------- 日志目录：每次运行单独子目录 ----------
RUN_TS="$(date +%Y%m%d_%H%M%S)"
LOG_ROOT="${SCRIPT_DIR}/log"
RUN_DIR="${LOG_ROOT}/run_${RUN_TS}"
mkdir -p "$RUN_DIR"

# 全文会话日志：终端 + run.log
exec > >(tee "${RUN_DIR}/run.log") 2>&1

echo "=========================================="
echo "Tracer 测试套件"
echo "工作目录: ${SCRIPT_DIR}"
echo "本次日志目录: ${RUN_DIR}"
echo "Go: $(command -v go) — $(go version 2>/dev/null || echo 'go 不可用')"
echo "GOROOT: $(go env GOROOT 2>/dev/null || echo 'n/a')"
echo "=========================================="
echo ""

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

_run_pkg_test() {
  local name="$1"
  local pkg="$2"
  local logfile="${RUN_DIR}/${name}.log"
  echo "测试 ${name} (${pkg})..."
  if go test -v "$pkg" 2>&1 | tee "$logfile"; then
    echo -e "${GREEN}✓ ${name} 测试通过${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
  else
    echo -e "${RED}✗ ${name} 测试失败${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
  fi
  TOTAL_TESTS=$((TOTAL_TESTS + 1))
  echo ""
}

echo "=========================================="
echo "1. 运行所有单元测试"
echo "=========================================="
echo ""

_run_pkg_test "exporter_test" "./exporter"
_run_pkg_test "sampler_test" "./sampler"
_run_pkg_test "metrics_test" "./metrics"
_run_pkg_test "processor_test" "./processor"
_run_pkg_test "core_test" "./core"
_run_pkg_test "propagation_test" "./propagation/..."
_run_pkg_test "integration_test" "./integration"

echo "=========================================="
echo "2. 生成测试覆盖率报告"
echo "=========================================="
echo ""

COVER_OUT="${RUN_DIR}/coverage.out"
COVER_HTML="${RUN_DIR}/coverage.html"
COVER_LOG="${RUN_DIR}/coverage_test.log"

echo "生成覆盖率数据..."
go test -coverprofile="$COVER_OUT" ./exporter ./sampler ./metrics ./processor ./core ./integration 2>&1 | tee "$COVER_LOG" || true
echo ""

echo "覆盖率统计："
go tool cover -func="$COVER_OUT" | tail -1
echo ""

echo "生成覆盖率 HTML 报告..."
go tool cover -html="$COVER_OUT" -o "$COVER_HTML"
echo -e "${GREEN}✓ 覆盖率报告: ${COVER_HTML}${NC}"
echo ""

echo "=========================================="
echo "3. 运行基准测试"
echo "=========================================="
echo ""

echo "运行 exporter 基准测试..."
go test -run '^$' -bench=. -benchmem -benchtime=3s ./exporter 2>&1 | tee "${RUN_DIR}/exporter_bench.log"
echo ""

echo "运行 processor 基准测试..."
go test -run '^$' -bench=. -benchmem -benchtime=3s ./processor 2>&1 | tee "${RUN_DIR}/processor_bench.log"
echo ""

echo "运行 core 基准测试..."
go test -run '^$' -bench=. -benchmem -benchtime=3s ./core 2>&1 | tee "${RUN_DIR}/core_bench.log"
echo ""

echo "运行 sampler 基准测试..."
go test -run '^$' -bench=. -benchmem -benchtime=3s ./sampler 2>&1 | tee "${RUN_DIR}/sampler_bench.log"
echo ""

echo "=========================================="
echo "4. 测试结果汇总"
echo "=========================================="
echo ""

echo -e "总测试包数: ${TOTAL_TESTS}"
echo -e "${GREEN}通过: ${PASSED_TESTS}${NC}"
if [ "$FAILED_TESTS" -gt 0 ]; then
  echo -e "${RED}失败: ${FAILED_TESTS}${NC}"
else
  echo -e "${GREEN}失败: ${FAILED_TESTS}${NC}"
fi
echo ""

echo "=========================================="
echo "5. 生成测试报告 (Markdown)"
echo "=========================================="
echo ""

REPORT_FILE="${RUN_DIR}/test_report.md"

{
  echo "# Tracer 测试报告"
  echo ""
  echo "生成时间: $(date)"
  echo ""
  echo "- 日志目录: \`${RUN_DIR}\`"
  echo "- Go: \`$(go version 2>/dev/null || echo n/a)\`"
  echo ""
  echo "## 测试结果汇总"
  echo ""
  echo "- 总测试包数: ${TOTAL_TESTS}"
  echo "- 通过: ${PASSED_TESTS}"
  echo "- 失败: ${FAILED_TESTS}"
  echo ""
  echo "## 测试覆盖率"
  echo ""
  echo '```'
  go tool cover -func="$COVER_OUT" | tail -1
  echo '```'
  echo ""
  echo "HTML 报告: \`${COVER_HTML}\`"
  echo ""
  echo "## 基准测试结果摘要（各日志末尾）"
  echo ""
  echo "### Exporter"
  echo '```'
  tail -30 "${RUN_DIR}/exporter_bench.log" 2>/dev/null || echo "(无)"
  echo '```'
  echo ""
  echo "### Processor"
  echo '```'
  tail -30 "${RUN_DIR}/processor_bench.log" 2>/dev/null || echo "(无)"
  echo '```'
  echo ""
  echo "### Core"
  echo '```'
  tail -30 "${RUN_DIR}/core_bench.log" 2>/dev/null || echo "(无)"
  echo '```'
  echo ""
  echo "### Sampler"
  echo '```'
  tail -30 "${RUN_DIR}/sampler_bench.log" 2>/dev/null || echo "(无)"
  echo '```'
  echo ""
  echo "## 本目录内详细日志"
  echo ""
  echo "| 文件 | 说明 |"
  echo "|------|------|"
  echo "| run.log | 本次运行完整终端输出 |"
  for f in exporter_test.log sampler_test.log metrics_test.log processor_test.log core_test.log propagation_test.log integration_test.log coverage_test.log; do
    echo "| ${f} | 对应包测试输出 |"
  done
  echo "| coverage.out / coverage.html | 覆盖率数据与 HTML |"
  echo "| *_bench.log | 各包基准测试 |"
} > "$REPORT_FILE"

echo -e "${GREEN}✓ Markdown 报告: ${REPORT_FILE}${NC}"
echo ""

echo "=========================================="
echo "测试完成"
echo "=========================================="
echo "日志与报告目录: ${RUN_DIR}"
echo "  - run.log        完整输出"
echo "  - test_report.md 汇总报告"
echo "=========================================="
