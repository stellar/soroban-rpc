package transactions

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/stellar/go/toid"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
)

type Cursor struct {
	// LedgerSequence is the sequence of the ledger which emitted the event.
	LedgerSequence uint32
	// TxIdx is the index of the transaction within the ledger which emitted the event.
	TxIdx uint32
	// Op is the index of the operation within the transaction which emitted the event.
	// Note: Currently, there is no use for it (events are transaction-wide and not operation-specific)
	//       but we keep it in order to make the API future-proof.
	OpIdx uint32
}

// String returns a string representation of this cursor
func (c *Cursor) String() string {
	return fmt.Sprintf(
		"%019d",
		toid.New(int32(c.LedgerSequence), int32(c.TxIdx), int32(c.OpIdx)).ToInt64(),
	)
}

// MarshalJSON marshals the cursor into JSON
func (c *Cursor) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

// UnmarshalJSON unmarshalls a cursor from the given JSON
func (c *Cursor) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	parsed, err := c.ParseCursor(s)
	if err != nil {
		return err
	}

	if txCursor, ok := parsed.(*Cursor); ok {
		*c = *txCursor
	} else {
		return fmt.Errorf("parsed cursor could not be converted to Transaction cursor")
	}
	return nil
}

// Cmp compares two cursors.
// 0 is returned if the c is equal to other.
// 1 is returned if c is greater than other.
// -1 is returned if c is less than other.
func (c *Cursor) Cmp(other interfaces.Cursor) int {
	otherCursor := other.(*Cursor)

	if c.LedgerSequence == otherCursor.LedgerSequence {
		return cmp(c.TxIdx, otherCursor.TxIdx)
	}
	return cmp(c.LedgerSequence, otherCursor.LedgerSequence)
}

func (c *Cursor) ParseCursor(input string) (interfaces.Cursor, error) {
	idInt, err := strconv.ParseInt(input, 10, 64) //lint:ignore gomnd
	if err != nil {
		return &Cursor{}, fmt.Errorf("invalid cursor %s: %w", input, err)
	}
	parsed := toid.Parse(idInt)
	return &Cursor{
		LedgerSequence: uint32(parsed.LedgerSequence),
		TxIdx:          uint32(parsed.TransactionOrder),
		OpIdx:          uint32(parsed.OperationOrder),
	}, nil
}

func cmp(a, b uint32) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func NewCursor(sequence uint32, txIdx uint32, opIdx uint32) Cursor {
	return Cursor{
		LedgerSequence: sequence,
		TxIdx:          txIdx,
		OpIdx:          opIdx,
	}
}
