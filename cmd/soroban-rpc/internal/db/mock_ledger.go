package db

import (
	"context"

	"github.com/stellar/go/xdr"
)

type mockLedgerReader struct {
	txn mockTransactionHandler
}

func NewMockLedgerReader(txn *mockTransactionHandler) *mockLedgerReader {
	return &mockLedgerReader{
		txn: *txn,
	}
}

func (m *mockLedgerReader) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	lcm := m.txn.ledgerSeqToMeta[sequence]
	return *lcm, true, nil
}

func (m *mockLedgerReader) StreamAllLedgers(ctx context.Context, f StreamLedgerFn) error {
	return nil
}
