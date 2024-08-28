package arrowops

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func BenchmarkRecordsEqual(b *testing.B) {
	for _, size := range TEST_SIZES {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mem := memory.NewGoAllocator()
				b.StopTimer()
				// create large records to compare
				r1 := MockData(mem, 1_000_000, "ascending")
				defer r1.Release()

				r2 := MockData(mem, 1_000_000, "ascending")
				defer r2.Release()

				b.StartTimer()

				if !array.RecordEqual(r1, r2) {
					b.Fatalf("expected records to be equal")
				} else {
					r1.Release()
					r2.Release()
				}
			}
		})
	}

}

func TestRecordsEqual(t *testing.T) {
	mem := memory.NewGoAllocator()
	// record to test
	rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float32},
			{Name: "c", Type: arrow.BinaryTypes.String},
		},
		nil,
	))
	defer rb1.Release()
	rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1, 2, 3}, nil)
	rb1.Field(1).(*array.Float32Builder).AppendValues([]float32{1.0, 2.0, 3.0}, nil)
	rb1.Field(2).(*array.StringBuilder).AppendValues([]string{"s1", "s2", "s3"}, nil)
	record := rb1.NewRecord()
	defer record.Release()
	// expected record
	rb2 := array.NewRecordBuilder(mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float32},
			{Name: "c", Type: arrow.BinaryTypes.String},
		},
		nil,
	))
	defer rb2.Release()
	rb2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1, 2, 3}, nil)
	rb2.Field(1).(*array.Float32Builder).AppendValues([]float32{1.0, 2.0, 3.0}, nil)
	rb2.Field(2).(*array.StringBuilder).AppendValues([]string{"s1", "s2", "s3"}, nil)
	expectedRecord := rb2.NewRecord()
	defer expectedRecord.Release()
	// compare records
	if !array.RecordEqual(record, expectedRecord) {
		t.Logf("record: %v", record)
		t.Logf("expectedRecord: %v", expectedRecord)
		t.Errorf("expected records to be equal")
	}
}
