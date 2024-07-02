-- +migrate Up

-- indexing table to find events in ledgers by contract_id
CREATE TABLE events(
    ledger_sequence   INTEGER NOT NULL,
    application_order INTEGER NOT NULL,
    contract_id       BLOB(32),
    event_type        INTEGER NOT NULL
);

CREATE INDEX idx_ledger_sequence ON events(ledger_sequence);
CREATE INDEX idx_contract_id ON events(contract_id);
CREATE INDEX idx_ledger_sequence_application_order ON events (ledger_sequence, application_order);

-- +migrate Down
drop table events cascade;
