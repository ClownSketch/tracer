package core

import (
	"github.com/ClownSketch/tracer/types"
)

// shouldExportRecord 根据 forceRecord 策略与 Span 状态判断是否应在 End 时导出。
func shouldExportRecord(state *spanState) bool {
	switch state.forceRecord.Load() {
	case types.RecordPolicyAlways:
		return true
	case types.RecordPolicyOnError:
		return spanStateHasError(state)
	default:
		return false
	}
}

// spanStateHasError 判断 Span 是否已记录错误（供 RecordPolicyOnError 使用）。
func spanStateHasError(state *spanState) bool {
	if v := state.errorDetail.Load(); v != nil {
		if errDetail, ok := v.(*types.ErrorDetail); ok && errDetail != nil {
			return true
		}
	}

	if st := state.status.Load(); st != nil && st.Code == types.StatusCodeError {
		return true
	}

	return false
}

// resolveRecordPolicyAtStart 合并 Start 选项与采样决策，得到初始 forceRecord。
// @param configured 配置的导出策略
// @param decision 采样决策
// @return 初始 forceRecord
func resolveRecordPolicyAtStart(configured uint32, decision types.SamplingDecision) uint32 {
	// 如果配置的导出策略不为 None，则返回配置的导出策略
	if configured != types.RecordPolicyNone {
		return configured
	}
	// 如果采样决策为 RecordAndSample，则返回 Always，否则返回 None
	if decision == types.SamplingDecisionRecordAndSample {
		// 记录并采样，则始终导出
		return types.RecordPolicyAlways
	}

	// 默认不导出
	return types.RecordPolicyNone
}
