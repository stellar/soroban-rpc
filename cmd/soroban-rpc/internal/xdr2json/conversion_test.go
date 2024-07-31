package xdr2json

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/xdr"
)

func TestBytesConversion(t *testing.T) {
	// Make a structure to encode
	pubkey := keypair.MustRandom()
	asset := xdr.MustNewCreditAsset("ABCD", pubkey.Address())

	json, err := ConvertInterface(asset)
	require.NoError(t, err)
	require.Equal(t, []byte{}, json)
}
