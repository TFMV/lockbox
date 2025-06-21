package lockbox

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// DetectCSVSchema reads a CSV file and attempts to infer an Arrow schema.
// It reads up to sample records to determine column types.
func DetectCSVSchema(path string, sample int) (*arrow.Schema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	headers, err := r.Read()
	if err != nil {
		return nil, err
	}
	if sample <= 0 {
		sample = 10
	}
	types := make([]arrow.DataType, len(headers))
	for i := 0; i < sample; i++ {
		row, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		for j, val := range row {
			t := detectValueType(val)
			types[j] = mergeArrowType(types[j], t)
		}
	}
	fields := make([]arrow.Field, len(headers))
	for i, name := range headers {
		typ := types[i]
		if typ == nil {
			typ = arrow.BinaryTypes.String
		}
		fields[i] = arrow.Field{Name: name, Type: typ, Nullable: true}
	}
	return arrow.NewSchema(fields, nil), nil
}

func detectValueType(v string) arrow.DataType {
	if _, err := strconv.ParseInt(v, 10, 64); err == nil {
		return arrow.PrimitiveTypes.Int64
	}
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return arrow.PrimitiveTypes.Float64
	}
	if _, err := time.Parse(time.RFC3339, v); err == nil {
		return arrow.FixedWidthTypes.Timestamp_s
	}
	if v == "true" || v == "false" {
		return arrow.FixedWidthTypes.Boolean
	}
	return arrow.BinaryTypes.String
}

func mergeArrowType(a, b arrow.DataType) arrow.DataType {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a.ID() != b.ID() {
		return arrow.BinaryTypes.String
	}
	return a
}
