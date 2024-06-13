package db

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func transactionMetaWithEvents(events ...xdr.ContractEvent) xdr.TransactionMeta {
	return xdr.TransactionMeta{
		V:          3,
		Operations: &[]xdr.OperationMeta{},
		V3: &xdr.TransactionMetaV3{
			SorobanMeta: &xdr.SorobanTransactionMeta{
				Events: events,
			},
		},
	}
}

func contractEvent(contractID xdr.Hash, topic []xdr.ScVal, body xdr.ScVal) xdr.ContractEvent {
	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topic,
				Data:   body,
			},
		},
	}
}

func ledgerCloseMetaWithEvents(sequence uint32, closeTimestamp int64, txMeta ...xdr.TransactionMeta) xdr.LedgerCloseMeta {
	var txProcessing []xdr.TransactionResultMeta
	var phases []xdr.TransactionPhase

	var components []xdr.TxSetComponent
	for _, item := range txMeta {
		var operations []xdr.Operation
		for range item.MustV3().SorobanMeta.Events {
			operations = append(operations,
				xdr.Operation{
					Body: xdr.OperationBody{
						Type: xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
							HostFunction: xdr.HostFunction{
								Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
								InvokeContract: &xdr.InvokeContractArgs{
									ContractAddress: xdr.ScAddress{
										Type:       xdr.ScAddressTypeScAddressTypeContract,
										ContractId: &xdr.Hash{0x1, 0x2},
									},
									FunctionName: "foo",
									Args:         nil,
								},
							},
							Auth: []xdr.SorobanAuthorizationEntry{},
						},
					},
				})
		}
		envelope := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
					Operations:    operations,
				},
			},
		}
		txHash, err := network.HashTransactionInEnvelope(envelope, network.FutureNetworkPassphrase)
		if err != nil {
			panic(err)
		}

		txProcessing = append(txProcessing, xdr.TransactionResultMeta{
			TxApplyProcessing: item,
			Result: xdr.TransactionResultPair{
				TransactionHash: txHash,
			},
		})
		components = []xdr.TxSetComponent{
			{
				Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
				TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
					Txs: []xdr.TransactionEnvelope{
						envelope,
					},
				},
			},
		}
		phases = append(phases, xdr.TransactionPhase{
			V:            0,
			V0Components: &components,
		})
	}

	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: xdr.Hash{},
				Header: xdr.LedgerHeader{
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(closeTimestamp),
					},
					LedgerSeq: xdr.Uint32(sequence),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					PreviousLedgerHash: xdr.Hash{},
					Phases: []xdr.TransactionPhase{{
						V:            0,
						V0Components: &components,
					}},
				},
			},
			TxProcessing: txProcessing,
		},
	}
}

func TestInsertEvents(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)
	now := time.Now().UTC()

	writer := NewReadWriter(log, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)
	contractID := xdr.Hash([32]byte{})
	counter := xdr.ScSymbol("COUNTER")

	var txMeta []xdr.TransactionMeta
	for i := 0; i < 10; i++ {
		txMeta = append(txMeta, transactionMetaWithEvents(
			contractEvent(
				contractID,
				xdr.ScVec{xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  &counter,
				}},
				xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  &counter,
				},
			),
		))
	}
	ledgerCloseMeta := ledgerCloseMetaWithEvents(1, now.Unix(), txMeta...)

	eventW := write.EventWriter()
	err = eventW.InsertEvents(ledgerCloseMeta)
	assert.NoError(t, err)

	//TODO: Call getEvents and validate events data.
}
