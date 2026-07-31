package baggage

import (
	"testing"

	"github.com/ClownSketch/tracer/attribute"
)

func TestExtractFromAttributesKeepsOriginalValues(t *testing.T) {
	inherited := map[string]attribute.Attribute{
		"authorization": {
			Key:   "authorization",
			Value: attribute.StringValue("Bearer original-token"),
			Type:  attribute.AttributeTypeInherited,
		},
	}
	global := map[string]attribute.Attribute{
		"secret_key": {
			Key:   "secret_key",
			Value: attribute.StringValue("original-secret"),
			Type:  attribute.AttributeTypeGlobal,
		},
	}

	result := ExtractFromAttributes(inherited, global)
	if result["i:authorization"] != "Bearer original-token" {
		t.Fatalf("继承属性被修改: %q", result["i:authorization"])
	}
	if result["g:secret_key"] != "original-secret" {
		t.Fatalf("全局属性被修改: %q", result["g:secret_key"])
	}
}
