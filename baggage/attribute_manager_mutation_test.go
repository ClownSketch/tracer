package baggage

import (
	"fmt"
	"sync"
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

func TestAttributeManagerReturnedMapsAreCopies(t *testing.T) {
	manager := NewAttributeManager()
	manager.AddAttribute("region", attribute.StringValue("IN"), attribute.Config{
		Type: attribute.AttributeTypeGlobal,
	})

	attributes := manager.GetGlobalAttributes()
	delete(attributes, "region")

	value, exists := manager.GetGlobalAttribute("region")
	if !exists || value.String() != "IN" {
		t.Fatalf("修改返回副本后内部属性异常: value=%v exists=%v", value, exists)
	}
}

func TestAttributeManagerConcurrentAccess(t *testing.T) {
	manager := NewAttributeManager()
	const workerCount = 64
	const rounds = 100

	var waitGroup sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		waitGroup.Add(1)
		go func(workerID int) {
			defer waitGroup.Done()

			key := fmt.Sprintf("worker_%d", workerID)
			for round := 0; round < rounds; round++ {
				value := attribute.IntValue(round)
				manager.AddAttribute(key, value, attribute.Config{Type: attribute.AttributeTypeInherited})
				if _, exists := manager.GetInheritedAttribute(key); !exists {
					t.Errorf("并发写入后属性不存在: %s", key)
					return
				}
				_ = manager.GetInheritableAttributes()
			}
		}(worker)
	}
	waitGroup.Wait()

	attributes := manager.GetInheritableAttributes()
	if len(attributes) != workerCount {
		t.Fatalf("并发写入属性数量=%d，期望=%d", len(attributes), workerCount)
	}
}
