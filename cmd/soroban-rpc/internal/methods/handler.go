package methods

import (
	"fmt"
	"strings"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
)

func NewHandler(fn any) jrpc2.Handler {
	fi, err := handler.Check(fn)
	if err != nil {
		panic(err)
	}
	// explicitly disable array arguments since otherwise we cannot add
	// new method arguments without breaking backwards compatibility with clients
	fi.AllowArray(false)
	return fi.Wrap()
}

var errInvalidFormat = fmt.Errorf(
	"expected %s for 'xdrFormat'",
	strings.Join([]string{XdrFormatBase64, XdrFormatJSON}, ", "))
