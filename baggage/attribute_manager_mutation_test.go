package baggage

import (
	"testing"

	"github.com/ClownSketch/tracer/attribute"
)

func TestAttributeManagerUpdateAndRemove(t *testing.T) {
	manager := NewAttributeManager()
	manager.AddAttribute("request_id", attribute.StringValue("first"), attribute.Config{
		Type: attribute.AttributeTypeInherited,
	})

	if !manager.UpdateAttribute("request_id", attribute.StringValue("second"), attribute.AttributeTypeInherited) {
		t.Fatal("expected existing attribute to be updated")
	}
	value, exists := manager.GetInheritedAttribute("request_id")
	if !exists || value.String() != "second" {
		t.Fatalf("updated value = %#v, exists = %v", value, exists)
	}
	if manager.UpdateAttribute("missing", attribute.StringValue("value"), attribute.AttributeTypeInherited) {
		t.Fatal("missing attribute must not be updated")
	}

	if !manager.RemoveAttribute("request_id", attribute.AttributeTypeInherited) {
		t.Fatal("expected existing attribute to be removed")
	}
	if _, exists := manager.GetInheritedAttribute("request_id"); exists {
		t.Fatal("removed attribute is still present")
	}
	if manager.RemoveAttribute("request_id", attribute.AttributeTypeInherited) {
		t.Fatal("removing an absent attribute must return false")
	}
}
