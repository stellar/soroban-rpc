-- +migrate Up

-- index to find transactions in ledgers by hash
CREATE TABLE transactions (
    hash BLOB PRIMARY KEY,
    ledger_sequence INTEGER NOT NULL,
    application_order INTEGER NOT NULL,
    -- possibly a useful search index for Soroban-oriented queries, e.g.
    -- transactions that involve the SAC or have just events in general
    is_soroban BOOLEAN NOT NULL,
    FOREIGN KEY (ledger_sequence)
        REFERENCES ledger_close_meta (sequence)
);

-- +migrate Down
drop table transactions cascade;
