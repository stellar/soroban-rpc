package db

import (
	"encoding/hex"

	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
)

func ledgerCloseTime(ledgerSequence uint32) int64 {
	return int64(ledgerSequence)*25 + 100
}

func TxHash(acctSeq uint32) xdr.Hash {
	envelope := TxEnvelope(acctSeq)
	hash, err := network.HashTransactionInEnvelope(envelope, network.FutureNetworkPassphrase)
	if err != nil {
		panic(err)
	}

	return hash
}

func TxEnvelope(acctSeq uint32) xdr.TransactionEnvelope {
	envelope, err := xdr.NewTransactionEnvelope(xdr.EnvelopeTypeEnvelopeTypeTx, xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			Fee:           1,
			SeqNum:        xdr.SequenceNumber(acctSeq),
			SourceAccount: xdr.MustMuxedAddress("MA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVAAAAAAAAAAAAAJLK"),
		},
	})
	if err != nil {
		panic(err)
	}
	return envelope
}

func TransactionResult(successful bool) xdr.TransactionResult {
	code := xdr.TransactionResultCodeTxBadSeq
	if successful {
		code = xdr.TransactionResultCodeTxSuccess
	}
	opResults := []xdr.OperationResult{}
	return xdr.TransactionResult{
		FeeCharged: 100,
		Result: xdr.TransactionResultResult{
			Code:    code,
			Results: &opResults,
		},
	}
}

func CreateTxMeta(acctSeq uint32, successful bool) xdr.LedgerCloseMeta {
	envelope := TxEnvelope(acctSeq)

	txProcessing := []xdr.TransactionResultMeta{
		{
			TxApplyProcessing: xdr.TransactionMeta{
				V:          3,
				Operations: &[]xdr.OperationMeta{},
				V3:         &xdr.TransactionMetaV3{},
			},
			Result: xdr.TransactionResultPair{
				TransactionHash: TxHash(acctSeq),
				Result:          TransactionResult(successful),
			},
		},
	}

	components := []xdr.TxSetComponent{
		{
			Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
				BaseFee: nil,
				Txs: []xdr.TransactionEnvelope{
					envelope,
				},
			},
		},
	}
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(ledgerCloseTime(acctSeq + 100)),
					},
					LedgerSeq: xdr.Uint32(acctSeq),
				},
			},
			TxProcessing: txProcessing,
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					PreviousLedgerHash: xdr.Hash{1},
					Phases: []xdr.TransactionPhase{
						{
							V:            0,
							V0Components: &components,
						},
					},
				},
			},
		},
	}
}

func TxMetaWithEvents(acctSeq uint32, successful bool) xdr.LedgerCloseMeta {

	meta := CreateTxMeta(acctSeq, successful)

	contractIDBytes, _ := hex.DecodeString("df06d62447fd25da07c0135eed7557e5a5497ee7d15b7fe345bd47e191d8f577")
	var contractID xdr.Hash
	copy(contractID[:], contractIDBytes)
	counter := xdr.ScSymbol("COUNTER")

	meta.V1.TxProcessing[0].TxApplyProcessing.V3 = &xdr.TransactionMetaV3{
		SorobanMeta: &xdr.SorobanTransactionMeta{
			Events: []xdr.ContractEvent{{
				ContractId: &contractID,
				Type:       xdr.ContractEventTypeContract,
				Body: xdr.ContractEventBody{
					V: 0,
					V0: &xdr.ContractEventV0{
						Topics: []xdr.ScVal{{
							Type: xdr.ScValTypeScvSymbol,
							Sym:  &counter,
						}},
						Data: xdr.ScVal{
							Type: xdr.ScValTypeScvSymbol,
							Sym:  &counter,
						},
					},
				},
			}},
			ReturnValue: xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &counter,
			},
		},
	}

	return meta
}
