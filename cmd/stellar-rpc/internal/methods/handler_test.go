package methods

import (
	"context"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stretchr/testify/assert"
)

type Request struct {
	Parameter string `json:"parameter"`
}

func TestNewHandlerNoArrayParameters(t *testing.T) {
	callCount := 0
	f := func(ctx context.Context, request Request) error {
		callCount++
		assert.Equal(t, "bar", request.Parameter)
		return nil
	}
	objectRequest := `{
"jsonrpc": "2.0",
"id": 1,
"method": "foo",
"params": { "parameter": "bar" }
}`
	requests, err := jrpc2.ParseRequests([]byte(objectRequest))
	assert.NoError(t, err)
	assert.Len(t, requests, 1)
	finalObjectRequest := requests[0].ToRequest()

	// object parameters should work with our handlers
	customHandler := NewHandler(f)
	_, err = customHandler(context.Background(), finalObjectRequest)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)

	arrayRequest := `{
"jsonrpc": "2.0",
"id": 1,
"method": "foo",
"params": ["bar"]
}`
	requests, err = jrpc2.ParseRequests([]byte(arrayRequest))
	assert.NoError(t, err)
	assert.Len(t, requests, 1)
	finalArrayRequest := requests[0].ToRequest()

	// Array requests should work with the normal handler, but not with our handlers
	stdHandler := handler.New(f)
	_, err = stdHandler(context.Background(), finalArrayRequest)
	assert.NoError(t, err)
	assert.Equal(t, 2, callCount)

	_, err = customHandler(context.Background(), finalArrayRequest)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid parameters")
}
