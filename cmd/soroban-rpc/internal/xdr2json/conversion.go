//nolint:lll
package xdr2json

/*
// See preflight.go for add'l explanations:
// Note: no blank lines allowed.
#include <stdlib.h>
#include "../../lib/preflight.h"
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

// ConvertBytes takes an XDR object (`xdr`) and its serialized bytes (`field`)
// and returns the raw JSON-formatted serialization of that object.
// It can be unmarshalled to a proper JSON structure, but the raw bytes are
// returned to avoid unnecessary round-trips. If there is an
// error, it returns an empty JSON object.
//
// The `xdr` object does not need to actually be initialized/valid:
// we only use it to determine the name of the structure. We could just
// accept a string, but that would make mistakes likelier than passing the
// structure itself (by reference).
func ConvertBytes(xdr interface{}, field []byte) ([]byte, error) {
	xdrTypeName := reflect.TypeOf(xdr).Name()
	return convertAnyBytes(xdrTypeName, field)
}

// ConvertInterface takes a valid XDR object (`xdr`) and returns
// the raw JSON-formatted serialization of that object. If there is an
// error, it returns an empty JSON object.
//
// Unlike `ConvertBytes`, the value here needs to be valid and
// serializable.
func ConvertInterface(xdr interface{}) (json.RawMessage, error) {
	xdrTypeName := reflect.TypeOf(xdr).Name()
	if cerealXdr, ok := xdr.(encoding.BinaryMarshaler); ok {
		data, err := cerealXdr.MarshalBinary()
		if err != nil {
			return []byte("{}"), errors.Wrapf(err, "failed to serialize XDR type '%s'", xdrTypeName)
		}

		return convertAnyBytes(xdrTypeName, data)
	}

	return []byte("{}"), fmt.Errorf("expected serializable XDR, got '%s': %+v", xdrTypeName, xdr)
}

func convertAnyBytes(xdrTypeName string, field []byte) (json.RawMessage, error) {
	var jsonStr, errStr string
	// scope just added to show matching alloc/frees
	{
		goRawXdr := CXDR(field)
		b := C.CString(xdrTypeName)

		result := C.xdr_to_json(b, goRawXdr)
		C.free(unsafe.Pointer(b))

		jsonStr = C.GoString(result.json)
		errStr = C.GoString(result.error)

		C.free_conversion_result(result)
	}

	if errStr != "" {
		return json.RawMessage(jsonStr), errors.New(errStr)
	}

	return json.RawMessage(jsonStr), nil
}

// CXDR is ripped directly from preflight.go to avoid a dependency.
func CXDR(xdr []byte) C.xdr_t {
	return C.xdr_t{
		xdr: (*C.uchar)(C.CBytes(xdr)),
		len: C.size_t(len(xdr)),
	}
}

func TransactionToJSON(tx db.Transaction) (
	[]byte,
	[]byte,
	[]byte,
	error,
) {
	var err error
	var result, resultMeta, envelope []byte

	result, err = ConvertBytes(xdr.TransactionResult{}, tx.Result)
	if err != nil {
		return result, envelope, resultMeta, err
	}

	envelope, err = ConvertBytes(xdr.TransactionEnvelope{}, tx.Envelope)
	if err != nil {
		return result, envelope, resultMeta, err
	}

	resultMeta, err = ConvertBytes(xdr.TransactionMeta{}, tx.Meta)
	if err != nil {
		return result, envelope, resultMeta, err
	}

	return result, envelope, resultMeta, nil
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
