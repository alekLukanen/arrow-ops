package arrowops

import (
	"fmt"

	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/float16"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

/*
Take all rows from the input record based on the input indices record. The indices record should contain two UINT32 columns.
The first column should contain the index of the record in the "records" slice and the second column should contain
the index of the row in that record. The record returned contains data copied from the original record.
*/
func TakeMultipleRecords(mem *memory.GoAllocator, records []arrow.Record, indices arrow.Record) (arrow.Record, error) {
	for _, record := range records {
		record.Retain()
	}
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()

	if len(records) == 0 {
		return nil, errs.NewStackError(fmt.Errorf("%w| empty records slice", ErrNoDataSupplied))
	}

	// validate the indices record
	if indices.NumCols() != 2 {
		return nil, errs.NewStackError(fmt.Errorf(
			"%w| indices must have 2 columns, got %d",
			ErrColumnNotFound,
			indices.NumCols(),
		))
	}
	for idx := int64(0); idx < indices.NumCols(); idx++ {
		column := indices.Column(int(idx))
		if column.DataType().ID() != arrow.UINT32 {
			return nil, errs.NewStackError(fmt.Errorf(
				"%w| expected UINT32 column, got %s for column index %d",
				ErrUnsupportedDataType,
				column.DataType().Name(),
				idx,
			))
		}
	}

	// validate that the indices are within bounds
	recordSliceIndices := indices.Column(0).(*array.Uint32)
	recordIndices := indices.Column(1).(*array.Uint32)

	if recordSliceIndices.NullN() > 0 || recordIndices.NullN() > 0 {
		return nil, errs.NewStackError(fmt.Errorf("%w| null values are not allowed in the indices record", ErrNullValuesNotAllowed))
	}

	for idx := range indices.NumRows() {
		if int(recordSliceIndices.Value(int(idx))) >= len(records) {
			return nil, errs.NewStackError(fmt.Errorf("%w| record slice index out of bounds", ErrIndexOutOfBounds))
		}
	}

	for idx := range indices.NumRows() {
		if int(recordIndices.Value(int(idx))) >= int(records[recordSliceIndices.Value(int(idx))].NumRows()) {
			return nil, errs.NewStackError(fmt.Errorf(
				"%w| record slice index %d, row in record %d",
				ErrIndexOutOfBounds,
				recordSliceIndices.Value(int(idx)),
				recordIndices.Value(int(idx))),
			)
		}
	}

	// validate that all of the records have the same schema
	for idx := 1; idx < len(records); idx++ {
		if !RecordSchemasEqual(records[0], records[idx]) {
			return nil, errs.NewStackError(fmt.Errorf("%w| records have different schemas, record[0] and record[%d]", ErrSchemasNotEqual, idx))
		}
	}

	takenArrays := make([]arrow.Array, records[0].NumCols())
	for colIdx := 0; colIdx < int(records[0].NumCols()); colIdx++ {

		// get refs for each of the arrays in all records
		arrays := make([]arrow.Array, len(records))
		for recIdx, rec := range records {
			arrays[recIdx] = rec.Column(colIdx)
		}

		takenArray, takeErr := TakeMultipleArrays(mem, arrays, indices)
		if takeErr != nil {
			return nil, errs.NewStackError(fmt.Errorf("%w| failed to take multiple arrays", takeErr))
		}
		takenArrays[colIdx] = takenArray

	}

	resultRecord := array.NewRecord(records[0].Schema(), takenArrays, int64(recordIndices.Len()))
	return resultRecord, nil
}

func TakeMultipleArrays(mem *memory.GoAllocator, arrs []arrow.Array, indices arrow.Record) (arrow.Array, error) {
	switch arrs[0].DataType().ID() {
	case arrow.BOOL:

		return takeBoolArrays(mem, arrs, indices)
	case arrow.INT8:
		return takeNativeArrays[int8, *array.Int8](array.NewInt8Builder(mem), arrs, indices)
	case arrow.INT16:
		return takeNativeArrays[int16, *array.Int16](array.NewInt16Builder(mem), arrs, indices)
	case arrow.INT32:
		return takeNativeArrays[int32, *array.Int32](array.NewInt32Builder(mem), arrs, indices)
	case arrow.INT64:
		return takeNativeArrays[int64, *array.Int64](array.NewInt64Builder(mem), arrs, indices)
	case arrow.UINT8:
		return takeNativeArrays[uint8, *array.Uint8](array.NewUint8Builder(mem), arrs, indices)
	case arrow.UINT16:
		return takeNativeArrays[uint16, *array.Uint16](array.NewUint16Builder(mem), arrs, indices)
	case arrow.UINT32:
		return takeNativeArrays[uint32, *array.Uint32](array.NewUint32Builder(mem), arrs, indices)
	case arrow.UINT64:
		return takeNativeArrays[uint64, *array.Uint64](array.NewUint64Builder(mem), arrs, indices)
	case arrow.FLOAT16:
		return takeNativeArrays[float16.Num, *array.Float16](array.NewFloat16Builder(mem), arrs, indices)
	case arrow.FLOAT32:
		return takeNativeArrays[float32, *array.Float32](array.NewFloat32Builder(mem), arrs, indices)
	case arrow.FLOAT64:
		return takeNativeArrays[float64, *array.Float64](array.NewFloat64Builder(mem), arrs, indices)
	case arrow.STRING:
		return takeNativeArrays[string, *array.String](array.NewStringBuilder(mem), arrs, indices)
	case arrow.BINARY:
		return takeBinaryArrays(mem, arrs, indices)
	case arrow.DATE32:
		return takeNativeArrays[arrow.Date32, *array.Date32](array.NewDate32Builder(mem), arrs, indices)
	case arrow.DATE64:
		return takeNativeArrays[arrow.Date64, *array.Date64](array.NewDate64Builder(mem), arrs, indices)
	case arrow.TIMESTAMP:
		return takeNativeArrays[arrow.Timestamp, *array.Timestamp](
			array.NewTimestampBuilder(mem, arrs[0].DataType().(*arrow.TimestampType)), arrs, indices,
		)
	case arrow.TIME32:
		return takeNativeArrays[arrow.Time32, *array.Time32](
			array.NewTime32Builder(mem, arrs[0].DataType().(*arrow.Time32Type)), arrs, indices,
		)
	case arrow.TIME64:
		return takeNativeArrays[arrow.Time64, *array.Time64](
			array.NewTime64Builder(mem, arrs[0].DataType().(*arrow.Time64Type)), arrs, indices,
		)
	case arrow.DURATION:
		return takeNativeArrays[arrow.Duration, *array.Duration](
			array.NewDurationBuilder(mem, arrs[0].DataType().(*arrow.DurationType)), arrs, indices,
		)
	default:
		return nil, errs.NewStackError(ErrUnsupportedDataType)
	}
}

func takeBoolArrays(mem *memory.GoAllocator, arr []arrow.Array, indices arrow.Record) (*array.Boolean, error) {

	booleanArrays := make([]*array.Boolean, len(arr))
	for idx, a := range arr {
		booleanArrays[idx] = a.(*array.Boolean)
	}

	recordSliceIndices := indices.Column(0).(*array.Uint32)
	recordIndices := indices.Column(1).(*array.Uint32)

	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.Reserve(int(indices.NumRows()))
	for i := 0; i < int(indices.NumRows()); i++ {
		recIdx := int(recordSliceIndices.Value(i))
		rowIdx := int(recordIndices.Value(i))
		b.Append(booleanArrays[recIdx].Value(rowIdx))
	}

	return b.NewBooleanArray(), nil
}

func takeNativeArrays[T comparable, E valueArray[T]](b arrayBuilder[T], arrs []arrow.Array, indices arrow.Record) (E, error) {
	defer b.Release()

	EArrays := make([]E, len(arrs))
	for idx, a := range arrs {
		EArrays[idx] = a.(E)
	}

	recordSliceIndices := indices.Column(0).(*array.Uint32)
	recordIndices := indices.Column(1).(*array.Uint32)

	b.Reserve(int(indices.NumRows()))
	for i := 0; i < int(indices.NumRows()); i++ {
		recIdx := int(recordSliceIndices.Value(i))
		rowIdx := int(recordIndices.Value(i))
		b.Append(EArrays[recIdx].Value(rowIdx))
	}
	return b.NewArray().(E), nil
}

func takeBinaryArrays(mem *memory.GoAllocator, arr []arrow.Array, indices arrow.Record) (*array.Binary, error) {
	binaryArrays := make([]*array.Binary, len(arr))
	for idx, a := range arr {
		binaryArrays[idx] = a.(*array.Binary)
	}

	recordSliceIndices := indices.Column(0).(*array.Uint32)
	recordIndices := indices.Column(1).(*array.Uint32)

	b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()
	b.Reserve(int(indices.NumRows()))
	for i := 0; i < int(indices.NumRows()); i++ {
		recIdx := int(recordSliceIndices.Value(i))
		rowIdx := int(recordIndices.Value(i))
		b.Append(binaryArrays[recIdx].Value(rowIdx))
	}
	return b.NewBinaryArray(), nil
}
