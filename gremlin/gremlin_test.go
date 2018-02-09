package gremlin

import (
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestAddProperty(t *testing.T) {
	id, _ := uuid.NewV4()
	v := Vertex{
		ID:    id,
		Label: "foo",
	}
	v.AddProperty("prop1", 1)
	v.AddProperty("prop1", 2)
	v.AddProperty("prop2", 1)

	expectedProps := map[string][]Property{
		"prop1": []Property{
			Property{Value: 1},
			Property{Value: 2},
		},
		"prop2": []Property{
			Property{Value: 1},
		},
	}

	assert.Equal(t, expectedProps, v.Properties, "")

}
