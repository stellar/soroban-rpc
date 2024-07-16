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
	"fmt"
	"reflect"
	"unsafe"

	"github.com/pkg/errors"
)

func XdrToJson(xdr interface{}) (string, error) {
	xdrTypeName := reflect.TypeOf(xdr).Name()

	if encodableXdr, ok := xdr.(encoding.BinaryMarshaler); ok {
		rawXdr, err := encodableXdr.MarshalBinary()
		if err != nil {
			return "", errors.Wrapf(err,
				"failed to marshal XDR type '%s'", xdrTypeName)
		}

		// scoped just to show alloc/free mirroring:
		// type and return value strings
		{
			goRawXdr := CXDR(rawXdr)
			b := C.CString(xdrTypeName)

			result := C.xdr_to_json(b, goRawXdr)

			C.free(unsafe.Pointer(b))
			goStr := C.GoString(result)
			C.free(unsafe.Pointer(result))

			return goStr, nil
		}
	}

	return "", fmt.Errorf("expected XDR type, got '%s'", xdrTypeName)
}
