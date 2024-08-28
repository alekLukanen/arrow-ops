package arrowops

import (
  "time"
  "strconv"
  "math/rand"

  "github.com/apache/arrow/go/v17/arrow"
  "github.com/apache/arrow/go/v17/arrow/array"
  "github.com/apache/arrow/go/v17/arrow/memory"
)

func MockData(mem *memory.GoAllocator, size int, method string) arrow.Record {
	fields := make([]arrow.Field, 0)
	fields = append(fields, arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Uint32})
	fields = append(fields, arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float32})
	fields = append(fields, arrow.Field{Name: "c", Type: arrow.BinaryTypes.String})

	rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(fields, nil))
	defer rb1.Release()

	currentTime := time.Now().UTC()

	aValues := make([]uint32, size)
	bValues := make([]float32, size)
	cValues := make([]string, size)

	updatedValues := make([]arrow.Timestamp, size)
	createdValues := make([]arrow.Timestamp, size)
	processedValues := make([]arrow.Timestamp, size)

	if method == "ascending" {
		for i := 0; i < size; i++ {
			aValues[i] = uint32(i)
			bValues[i] = float32(i)
			cValues[i] = strconv.Itoa(i)

			nextTs, err := arrow.TimestampFromTime(currentTime.Add(time.Duration(i)*time.Second), arrow.Millisecond)
			if err != nil {
				panic(err)
			}
			createdValues[i] = nextTs
			updatedValues[i] = nextTs
			processedValues[i] = nextTs
		}
	} else if method == "descending" {
		for i := 0; i < size; i++ {
			aValues[i] = uint32(size - 1 - i)
			bValues[i] = float32(size - 1 - i)
			cValues[i] = strconv.Itoa(size - 1 - i)

			nextTs, err := arrow.TimestampFromTime(currentTime.Add(-1*time.Duration(i)*time.Second), arrow.Millisecond)
			if err != nil {
				panic(err)
			}
			createdValues[i] = nextTs
			updatedValues[i] = nextTs
			processedValues[i] = nextTs
		}
	} else if method == "random" {
		for i := 0; i < size; i++ {
			val := rand.Intn(size)
			aValues[i] = uint32(val)
			bValues[i] = float32(val)
			cValues[i] = strconv.Itoa(val)

			nextTs, err := arrow.TimestampFromTime(
				currentTime.Add(-1*time.Duration(size)).Add(time.Duration(val)*time.Second),
				arrow.Millisecond,
			)
			if err != nil {
				panic(err)
			}
			createdValues[i] = nextTs
			updatedValues[i] = nextTs
			processedValues[i] = nextTs
		}
	} else {
		panic("invalid method")
	}

	rb1.Field(0).(*array.Uint32Builder).AppendValues(aValues, nil)
	rb1.Field(1).(*array.Float32Builder).AppendValues(bValues, nil)
	rb1.Field(2).(*array.StringBuilder).AppendValues(cValues, nil)

	return rb1.NewRecord()
}

