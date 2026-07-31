package fallback

import (
	"reflect"
	"testing"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/types"
)

func TestSpanLogFieldsPreserveJSONCompatibleTypes(t *testing.T) {
	testCases := []struct {
		name   string
		fields any
	}{
		{
			name:   "string",
			fields: "upstream timeout",
		},
		{
			name:   "slice",
			fields: []any{"channel_a", float64(2), true},
		},
	}

	converters := []struct {
		name    string
		convert func(span *mock.SpanSnapshotMock) ([]byte, error)
	}{
		{
			name: "fallback",
			convert: func(span *mock.SpanSnapshotMock) ([]byte, error) {
				return ConvertSpanSnapshotToJSON(span)
			},
		},
		{
			name: "wal",
			convert: func(span *mock.SpanSnapshotMock) ([]byte, error) {
				return ConvertSpanSnapshotToWALJSON(span)
			},
		},
	}

	for _, converter := range converters {
		for _, testCase := range testCases {
			t.Run(converter.name+"/"+testCase.name, func(t *testing.T) {
				span := mock.NewSpanSnapshotMock(1)
				span.Logs = []types.SpanLog{
					{
						Message:  "test log",
						Severity: types.SpanLogSeverityInfo,
						Fields:   testCase.fields,
					},
				}

				data, err := converter.convert(span)
				if err != nil {
					t.Fatalf("序列化 Span 失败: %v", err)
				}
				restored, err := ConvertJSONToSpanSnapshot(data)
				if err != nil {
					t.Fatalf("恢复 Span 失败: %v", err)
				}
				defer restored.Release()

				logs := restored.GetLogs()
				if len(logs) != 1 {
					t.Fatalf("恢复日志数量错误: %d", len(logs))
				}
				if !reflect.DeepEqual(logs[0].Fields, testCase.fields) {
					t.Fatalf("Fields 恢复结果错误: want=%#v got=%#v", testCase.fields, logs[0].Fields)
				}
			})
		}
	}
}
