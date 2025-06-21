package lockbox

import "testing"

func TestDetectCSVSchema(t *testing.T) {
	schema, err := DetectCSVSchema("../../data.csv", 2)
	if err != nil {
		t.Fatalf("detect error: %v", err)
	}
	if schema == nil || len(schema.Fields()) == 0 {
		t.Fatalf("expected schema")
	}
}
