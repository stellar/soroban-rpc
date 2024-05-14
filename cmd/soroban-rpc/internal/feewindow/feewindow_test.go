package feewindow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputeFeeDistribution(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		input  []uint64
		output FeeDistribution
	}{
		{"nil", nil, FeeDistribution{}},
		{"empty", []uint64{}, FeeDistribution{}},
		{"one",
			[]uint64{100},
			FeeDistribution{
				Max:      100,
				Min:      100,
				Mode:     100,
				P10:      100,
				P20:      100,
				P30:      100,
				P40:      100,
				P50:      100,
				P60:      100,
				P70:      100,
				P80:      100,
				P90:      100,
				P95:      100,
				P99:      100,
				FeeCount: 1,
			},
		},
		{"even number of elements: four 100s and six 1000s",
			[]uint64{100, 100, 100, 1000, 100, 1000, 1000, 1000, 1000, 1000},
			FeeDistribution{
				Max:      1000,
				Min:      100,
				Mode:     1000,
				P10:      100,
				P20:      100,
				P30:      100,
				P40:      100,
				P50:      1000,
				P60:      1000,
				P70:      1000,
				P80:      1000,
				P90:      1000,
				P95:      1000,
				P99:      1000,
				FeeCount: 10,
			},
		},
		{"odd number of elements: five 100s and six 1000s",
			[]uint64{100, 100, 100, 1000, 100, 1000, 1000, 1000, 1000, 1000, 100},
			FeeDistribution{
				Max:      1000,
				Min:      100,
				Mode:     1000,
				P10:      100,
				P20:      100,
				P30:      100,
				P40:      100,
				P50:      1000,
				P60:      1000,
				P70:      1000,
				P80:      1000,
				P90:      1000,
				P95:      1000,
				P99:      1000,
				FeeCount: 11,
			},
		},
		{"mutiple modes favors the smallest value",
			[]uint64{100, 1000},
			FeeDistribution{
				Max:      1000,
				Min:      100,
				Mode:     100,
				P10:      100,
				P20:      100,
				P30:      100,
				P40:      100,
				P50:      100,
				P60:      1000,
				P70:      1000,
				P80:      1000,
				P90:      1000,
				P95:      1000,
				P99:      1000,
				FeeCount: 2,
			},
		},
	} {
		assert.Equal(t, computeFeeDistribution(testCase.input, 0), testCase.output, testCase.name)
	}
}
