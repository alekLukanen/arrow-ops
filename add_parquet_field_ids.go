package arrowops

import (
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

func AddParquetFieldIds(record arrow.Record) arrow.Record {
	schema := record.Schema()
	metadata := schema.Metadata()
	fields := make([]arrow.Field, schema.NumFields())
	for i, field := range schema.Fields() {
		fields[i] = arrow.Field{
			Name:     field.Name,
			Type:     field.Type,
			Nullable: field.Nullable,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{strconv.Itoa(i)}),
		}
	}
	newSchema := arrow.NewSchema(fields, &metadata)
	return array.NewRecord(newSchema, record.Columns(), record.NumRows())
}
