package feewindow

import (
	"io"
	"slices"
	"sync"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
)

type FeeDistribution struct {
	Max   uint64
	Min   uint64
	Mode  uint64
	P10   uint64
	P20   uint64
	P30   uint64
	P40   uint64
	P50   uint64
	P60   uint64
	P70   uint64
	P80   uint64
	P90   uint64
	P95   uint64
	P99   uint64
	Count uint64
}

type FeeWindow struct {
	lock          sync.RWMutex
	totalFeeCount uint64
	feesPerLedger *ledgerbucketwindow.LedgerBucketWindow[[]uint64]
	distribution  FeeDistribution
}

func NewFeeWindow(retentionWindow uint32) *FeeWindow {
	window := ledgerbucketwindow.NewLedgerBucketWindow[[]uint64](retentionWindow)
	return &FeeWindow{
		totalFeeCount: 0,
		feesPerLedger: window,
	}
}

func (fw *FeeWindow) AppendLedgerFees(fees ledgerbucketwindow.LedgerBucket[[]uint64]) error {
	fw.lock.Lock()
	defer fw.lock.Unlock()
	_, err := fw.feesPerLedger.Append(fees)
	if err != nil {
		return err
	}

	var allFees []uint64
	for i := uint32(0); i < fw.feesPerLedger.Len(); i++ {
		allFees = append(allFees, fw.feesPerLedger.Get(i).BucketContent...)
	}
	fw.distribution = computeFeeDistribution(allFees)

	return nil
}

func computeFeeDistribution(fees []uint64) FeeDistribution {
	if len(fees) == 0 {
		return FeeDistribution{}
	}
	slices.Sort(fees)
	mode := fees[0]
	lastVal := fees[0]
	maxRepetitions := 0
	localRepetitions := 0
	for i := 1; i < len(fees); i++ {
		if fees[i] == lastVal {
			localRepetitions += 1
			continue
		}

		// new cluster of values

		if localRepetitions > maxRepetitions {
			maxRepetitions = localRepetitions
			mode = fees[i]
		}
		lastVal = fees[i]
		localRepetitions = 0
	}
	count := uint64(len(fees))
	// nearest-rank percentile
	percentile := func(p uint64) uint64 {
		// ceiling(p*count/100)
		kth := ((p * count) + 100 - 1) / 100
		return fees[kth-1]
	}
	return FeeDistribution{
		Max:   fees[len(fees)-1],
		Min:   fees[0],
		Mode:  mode,
		P10:   percentile(10),
		P20:   percentile(20),
		P30:   percentile(30),
		P40:   percentile(40),
		P50:   percentile(50),
		P60:   percentile(60),
		P70:   percentile(70),
		P80:   percentile(80),
		P90:   percentile(90),
		P95:   percentile(95),
		P99:   percentile(99),
		Count: count,
	}
}

func (fw *FeeWindow) GetFeeDistribution() FeeDistribution {
	fw.lock.RLock()
	defer fw.lock.RUnlock()
	return fw.distribution
}

type FeeWindows struct {
	SorobanInclusionFeeWindow *FeeWindow
	ClassicFeeWindow          *FeeWindow
	networkPassPhrase         string
}

func NewFeeWindows(classicRetention uint32, sorobanRetetion uint32, networkPassPhrase string) *FeeWindows {
	return &FeeWindows{
		SorobanInclusionFeeWindow: NewFeeWindow(sorobanRetetion),
		ClassicFeeWindow:          NewFeeWindow(classicRetention),
		networkPassPhrase:         networkPassPhrase,
	}
}

func (fw *FeeWindows) IngestFees(meta xdr.LedgerCloseMeta) error {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(fw.networkPassPhrase, meta)
	if err != nil {
		return err
	}
	var sorobanInclusionFees []uint64
	var classicFees []uint64
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		ops := tx.Envelope.Operations()
		if len(ops) == 1 {
			switch ops[0].Body.Type {
			case xdr.OperationTypeInvokeHostFunction, xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
				if tx.Envelope.V1 == nil || tx.Envelope.V1.Tx.Ext.SorobanData != nil {
					// this shouldn't happen
					continue
				}
				inclusionFee := uint64(tx.Envelope.V1.Tx.Fee) - uint64(tx.Envelope.V1.Tx.Ext.SorobanData.ResourceFee)
				classicFees = append(sorobanInclusionFees, inclusionFee)
				continue
			}
		}
		classicFees = append(classicFees, uint64(tx.Envelope.Fee()))

	}
	bucket := ledgerbucketwindow.LedgerBucket[[]uint64]{
		LedgerSeq:            meta.LedgerSequence(),
		LedgerCloseTimestamp: meta.LedgerCloseTime(),
		BucketContent:        classicFees,
	}
	if err := fw.ClassicFeeWindow.AppendLedgerFees(bucket); err != nil {
		return err
	}
	bucket.BucketContent = sorobanInclusionFees
	if err := fw.SorobanInclusionFeeWindow.AppendLedgerFees(bucket); err != nil {
		return err
	}
	return nil
}
