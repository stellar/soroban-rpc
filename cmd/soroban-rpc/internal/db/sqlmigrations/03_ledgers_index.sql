-- +migrate Up

-- creating an index in the ledgers table on the sequence number.
CREATE INDEX index_lcm_sequence ON ledger_close_meta(sequence);

-- +migrate Down
DROP INDEX index_lcm_sequence;