package preflight

/*
// See preflight.go for explanations:

#include "../../lib/preflight.h"
#include "../../lib/xdrjson.h"
#include <stdlib.h>
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-pc-windows-gnu/release-with-panic-unwind/ -lpreflight -lntdll -static -lws2_32 -lbcrypt -luserenv
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-apple-darwin/release-with-panic-unwind/ -lpreflight -ldl -lm
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/../../../../target/aarch64-apple-darwin/release-with-panic-unwind/ -lpreflight -ldl -lm
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-unknown-linux-gnu/release-with-panic-unwind/ -lpreflight -ldl -lm
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/../../../../target/aarch64-unknown-linux-gnu/release-with-panic-unwind/ -lpreflight -ldl -lm
*/
import "C"

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/pkg/errors"
)

func XdrToJson(xdr interface{}, field []byte) (map[string]interface{}, error) {
	xdrTypeName := reflect.TypeOf(xdr).Name()
	goStr := XdrToString(xdrTypeName, field)

	var result map[string]interface{}
	err := json.Unmarshal([]byte(goStr), &result)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode as '%s'", xdrTypeName)
	}

	return result, nil
}

func AnyXdrToString(xdr interface{}) (string, error) {
	xdrTypeName := reflect.TypeOf(xdr).Name()
	if cerealXdr, ok := xdr.(encoding.BinaryMarshaler); !ok {
		data, err := cerealXdr.MarshalBinary()
		if err != nil {
			return "", errors.Wrapf(err, "")
		}

		return XdrToString(xdrTypeName, data), nil
	}

	return "", fmt.Errorf("expected serializable XDR, got '%s': %+v", xdrTypeName, xdr)
}

func XdrToString(xdrTypeName string, field []byte) string {
	var goStr string
	// scope just added to show matching alloc/frees
	{
		goRawXdr := CXDR(field)
		b := C.CString(xdrTypeName)

		result := C.xdr_to_json(b, goRawXdr)
		C.free(unsafe.Pointer(b))

		goStr = C.GoString(result)
		C.free(unsafe.Pointer(result))
	}

	return goStr
}
