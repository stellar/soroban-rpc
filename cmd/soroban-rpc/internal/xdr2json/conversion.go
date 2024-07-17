//nolint:lll
package xdr2json

/*
// See preflight.go for add'l explanations:

#include <stdlib.h>
#include "../../lib/xdrjson.h"

#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-pc-windows-gnu/release-with-panic-unwind/ -lpreflight -lntdll -static -lws2_32 -lbcrypt -luserenv
#cgo darwin,amd64  LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-apple-darwin/release-with-panic-unwind/ -lpreflight -ldl -lm
#cgo darwin,arm64  LDFLAGS: -L${SRCDIR}/../../../../target/aarch64-apple-darwin/release-with-panic-unwind/ -lpreflight -ldl -lm
#cgo linux,amd64   LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-unknown-linux-gnu/release-with-panic-unwind/ -lpreflight -ldl -lm
#cgo linux,arm64   LDFLAGS: -L${SRCDIR}/../../../../target/aarch64-unknown-linux-gnu/release-with-panic-unwind/ -lpreflight -ldl -lm
*/
import "C"

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

const (
	FormatBase64 = "base64"
	FormatJSON   = "json"
)

var errInvalidFormat = fmt.Errorf(
	"expected %s for optional 'xdrFormat'",
	strings.Join([]string{FormatBase64, FormatJSON}, ", "))

func Convert(xdr interface{}, field []byte) (map[string]interface{}, error) {
	xdrTypeName := reflect.TypeOf(xdr).Name()
	goStr := convertStr(xdrTypeName, field)
	return jsonify(goStr)
}

func ConvertAny(xdr interface{}) (map[string]interface{}, error) {
	jsonStr, err := convertAnyStr(xdr)
	if err != nil {
		return nil, err
	}

	return jsonify(jsonStr)
}

func convertAnyStr(xdr interface{}) (string, error) {
	xdrTypeName := reflect.TypeOf(xdr).Name()
	if cerealXdr, ok := xdr.(encoding.BinaryMarshaler); !ok {
		data, err := cerealXdr.MarshalBinary()
		if err != nil {
			return "", errors.Wrapf(err, "failed to serialize XDR type '%s'", xdrTypeName)
		}

		return convertStr(xdrTypeName, data), nil
	}

	return "", fmt.Errorf("expected serializable XDR, got '%s': %+v", xdrTypeName, xdr)
}

func convertStr(xdrTypeName string, field []byte) string {
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

func jsonify(s string) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := json.Unmarshal([]byte(s), &result)
	if err != nil {
		return nil, err
	} else if jsonErr, ok := result["error"]; ok {
		return nil, fmt.Errorf("error during conversion: %+v", jsonErr)
	}

	return result, nil
}

func CXDR(xdr []byte) C.xdr_t {
	return C.xdr_t{
		xdr: (*C.uchar)(C.CBytes(xdr)),
		len: C.size_t(len(xdr)),
	}
}

func TransactionToJSON(tx db.Transaction) (
	map[string]interface{},
	map[string]interface{},
	map[string]interface{},
	[]map[string]interface{},
	error,
) {
	var err error
	var result, envelope, resultMeta map[string]interface{}
	var diagEvents []map[string]interface{}

	result, err = Convert(xdr.TransactionResult{}, tx.Result)
	if err != nil {
		return result, envelope, resultMeta, diagEvents, err
	}

	envelope, err = Convert(xdr.TransactionEnvelope{}, tx.Envelope)
	if err != nil {
		return result, envelope, resultMeta, diagEvents, err
	}

	resultMeta, err = Convert(xdr.TransactionMeta{}, tx.Meta)
	if err != nil {
		return result, envelope, resultMeta, diagEvents, err
	}

	diagEvents = make([]map[string]interface{}, len(tx.Events))
	for i, event := range tx.Events {
		diagEvents[i], err = Convert(xdr.DiagnosticEvent{}, event)
		if err != nil {
			return result, envelope, resultMeta, diagEvents, err
		}
	}

	return result, envelope, resultMeta, diagEvents, nil
}

func IsValidConversion(format string) error {
	switch format {
	case "":
	case FormatJSON:
	case FormatBase64:
	default:
		return errInvalidFormat
	}
	return nil
}
