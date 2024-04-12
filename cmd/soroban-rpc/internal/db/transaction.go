package db

type TransactionReader interface{}

type transactionReader struct {
	db *DB
}

func NewTransactionReader(db *DB) TransactionReader {
	return transactionReader{db: db}
}

type TransactionWriter interface{}

type transactionWriter struct {
	db *DB
}

func NewTransactionWriter(db *DB) TransactionWriter {
	return transactionWriter{db: db}
}
