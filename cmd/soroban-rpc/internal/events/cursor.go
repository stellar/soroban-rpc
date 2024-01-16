package events

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/stellar/go/toid"
)

// Cursor represents the position of a Soroban event.
// Soroban events are sorted in ascending order by
// ledger sequence, transaction index, operation index,
// and event index.
type Cursor struct {
	// Ledger is the sequence of the ledger which emitted the event.
	Ledger uint32
	// Tx is the index of the transaction within the ledger which emitted the event.
	Tx uint32
	// Op is the index of the operation within the transaction which emitted the event.
	Op uint32
	// Event is the index of the event within in the operation which emitted the event.
	Event uint32
}

// String returns a string representation of this cursor
func (c Cursor) String() string {
	return fmt.Sprintf(
		"%019d-%010d",
		toid.New(int32(c.Ledger), int32(c.Tx), int32(c.Op)).ToInt64(),
		c.Event,
	)
}

// MarshalJSON marshals the cursor into JSON
func (c Cursor) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

// UnmarshalJSON unmarshalls a cursor from the given JSON
func (c *Cursor) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	if parsed, err := ParseCursor(s); err != nil {
		return err
	} else {
		*c = parsed
	}
	return nil
}

// ParseCursor parses the given string and returns the corresponding cursor
func ParseCursor(input string) (Cursor, error) {
	parts := strings.SplitN(input, "-", 2)
	if len(parts) != 2 {
		return Cursor{}, fmt.Errorf("invalid event id %s", input)
	}

	// Parse the first part (toid)
	idInt, err := strconv.ParseInt(parts[0], 10, 64) //lint:ignore gomnd
	if err != nil {
		return Cursor{}, fmt.Errorf("invalid event id %s: %w", input, err)
	}
	parsed := toid.Parse(idInt)

	// Parse the second part (event order)
	eventOrder, err := strconv.ParseUint(parts[1], 10, 32) //lint:ignore gomnd
	if err != nil {
		return Cursor{}, fmt.Errorf("invalid event id %s: %w", input, err)
	}

	return Cursor{
		Ledger: uint32(parsed.LedgerSequence),
		Tx:     uint32(parsed.TransactionOrder),
		Op:     uint32(parsed.OperationOrder),
		Event:  uint32(eventOrder),
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

// Cmp compares two cursors.
// 0 is returned if the c is equal to other.
// 1 is returned if c is greater than other.
// -1 is returned if c is less than other.
func (c Cursor) Cmp(other Cursor) int {
	if c.Ledger == other.Ledger {
		if c.Tx == other.Tx {
			if c.Op == other.Op {
				return cmp(c.Event, other.Event)
			}
			return cmp(c.Op, other.Op)
		}
		return cmp(c.Tx, other.Tx)
	}
	return cmp(c.Ledger, other.Ledger)
}

var (
	// MinCursor is the smallest possible cursor
	MinCursor = Cursor{}
	// MaxCursor is the largest possible cursor
	MaxCursor = Cursor{
		Ledger: math.MaxUint32,
		Tx:     math.MaxUint32,
		Op:     math.MaxUint32,
		Event:  math.MaxUint32,
	}
)
