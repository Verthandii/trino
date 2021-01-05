package trino

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type typeConverter struct {
	typeName   string
	parsedType []string // e.g. array, array, varchar, for [][]string
}

func newTypeConverter(typeName string) *typeConverter {
	return &typeConverter{
		typeName:   typeName,
		parsedType: parseType(typeName),
	}
}

// parses presto types, e.g. array(varchar(10)) to "array", "varchar"
// TODO: Use queryColumn.TypeSignature instead.
func parseType(name string) []string {
	parts := strings.Split(name, "(")
	if len(parts) == 1 {
		return parts
	}
	last := len(parts) - 1
	parts[last] = strings.TrimRight(parts[last], ")")
	if len(parts[last]) > 0 {
		if _, err := strconv.Atoi(parts[last]); err == nil {
			parts = parts[:last]
		}
	}
	return parts
}

// ConvertValue implements the driver.ValueConverter interface.
func (c *typeConverter) ConvertValue(v interface{}) (driver.Value, error) {
	switch c.parsedType[0] {
	case "boolean":
		vv, err := scanNullBool(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Bool, err
	case "json", "char", "varchar", "varbinary", "interval year to month", "interval day to second", "decimal", "ipaddress", "unknown":
		vv, err := scanNullString(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.String, err
	case "tinyint", "smallint", "integer", "bigint":
		vv, err := scanNullInt64(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Int64, err
	case "real", "double":
		vv, err := scanNullFloat64(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Float64, err
	case "date", "time", "time with time zone", "timestamp", "timestamp with time zone":
		vv, err := scanNullTime(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Time, err
	case "map":
		if err := validateMap(v); err != nil {
			return nil, err
		}
		return v, nil
	case "array":
		if err := validateSlice(v); err != nil {
			return nil, err
		}
		return v, nil
	default:
		return nil, fmt.Errorf("type not supported: %q", c.typeName)
	}
}

func validateMap(v interface{}) error {
	if v == nil {
		return nil
	}
	if _, ok := v.(map[string]interface{}); !ok {
		return fmt.Errorf("cannot convert %v (%T) to map", v, v)
	}
	return nil
}

func validateSlice(v interface{}) error {
	if v == nil {
		return nil
	}
	if _, ok := v.([]interface{}); !ok {
		return fmt.Errorf("cannot convert %v (%T) to slice", v, v)
	}
	return nil
}

func scanNullBool(v interface{}) (sql.NullBool, error) {
	if v == nil {
		return sql.NullBool{}, nil
	}
	vv, ok := v.(bool)
	if !ok {
		return sql.NullBool{},
			fmt.Errorf("cannot convert %v (%T) to bool", v, v)
	}
	return sql.NullBool{Valid: true, Bool: vv}, nil
}

// NullSliceBool represents a slice of bool that may be null.
type NullSliceBool struct {
	SliceBool []sql.NullBool
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceBool) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []bool", value, value)
	}
	slice := make([]sql.NullBool, len(vs))
	for i := range vs {
		v, err := scanNullBool(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceBool = slice
	s.Valid = true
	return nil
}

// NullSlice2Bool represents a two-dimensional slice of bool that may be null.
type NullSlice2Bool struct {
	Slice2Bool [][]sql.NullBool
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Bool) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]bool", value, value)
	}
	slice := make([][]sql.NullBool, len(vs))
	for i := range vs {
		var ss NullSliceBool
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceBool
	}
	s.Slice2Bool = slice
	s.Valid = true
	return nil
}

// NullSlice3Bool implements a three-dimensional slice of bool that may be null.
type NullSlice3Bool struct {
	Slice3Bool [][][]sql.NullBool
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Bool) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]bool", value, value)
	}
	slice := make([][][]sql.NullBool, len(vs))
	for i := range vs {
		var ss NullSlice2Bool
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Bool
	}
	s.Slice3Bool = slice
	s.Valid = true
	return nil
}

func scanNullString(v interface{}) (sql.NullString, error) {
	if v == nil {
		return sql.NullString{}, nil
	}
	vv, ok := v.(string)
	if !ok {
		return sql.NullString{},
			fmt.Errorf("cannot convert %v (%T) to string", v, v)
	}
	return sql.NullString{Valid: true, String: vv}, nil
}

// NullSliceString represents a slice of string that may be null.
type NullSliceString struct {
	SliceString []sql.NullString
	Valid       bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceString) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []string", value, value)
	}
	slice := make([]sql.NullString, len(vs))
	for i := range vs {
		v, err := scanNullString(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceString = slice
	s.Valid = true
	return nil
}

// NullSlice2String represents a two-dimensional slice of string that may be null.
type NullSlice2String struct {
	Slice2String [][]sql.NullString
	Valid        bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2String) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]string", value, value)
	}
	slice := make([][]sql.NullString, len(vs))
	for i := range vs {
		var ss NullSliceString
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceString
	}
	s.Slice2String = slice
	s.Valid = true
	return nil
}

// NullSlice3String implements a three-dimensional slice of string that may be null.
type NullSlice3String struct {
	Slice3String [][][]sql.NullString
	Valid        bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3String) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]string", value, value)
	}
	slice := make([][][]sql.NullString, len(vs))
	for i := range vs {
		var ss NullSlice2String
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2String
	}
	s.Slice3String = slice
	s.Valid = true
	return nil
}

func scanNullInt64(v interface{}) (sql.NullInt64, error) {
	if v == nil {
		return sql.NullInt64{}, nil
	}
	vNumber, ok := v.(json.Number)
	if !ok {
		return sql.NullInt64{},
			fmt.Errorf("cannot convert %v (%T) to int64", v, v)
	}
	vv, err := vNumber.Int64()
	if err != nil {
		return sql.NullInt64{},
			fmt.Errorf("cannot convert %v (%T) to int64", v, v)
	}
	return sql.NullInt64{Valid: true, Int64: vv}, nil
}

// NullSliceInt64 represents a slice of int64 that may be null.
type NullSliceInt64 struct {
	SliceInt64 []sql.NullInt64
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceInt64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []int64", value, value)
	}
	slice := make([]sql.NullInt64, len(vs))
	for i := range vs {
		v, err := scanNullInt64(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceInt64 = slice
	s.Valid = true
	return nil
}

// NullSlice2Int64 represents a two-dimensional slice of int64 that may be null.
type NullSlice2Int64 struct {
	Slice2Int64 [][]sql.NullInt64
	Valid       bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Int64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]int64", value, value)
	}
	slice := make([][]sql.NullInt64, len(vs))
	for i := range vs {
		var ss NullSliceInt64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceInt64
	}
	s.Slice2Int64 = slice
	s.Valid = true
	return nil
}

// NullSlice3Int64 implements a three-dimensional slice of int64 that may be null.
type NullSlice3Int64 struct {
	Slice3Int64 [][][]sql.NullInt64
	Valid       bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Int64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]int64", value, value)
	}
	slice := make([][][]sql.NullInt64, len(vs))
	for i := range vs {
		var ss NullSlice2Int64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Int64
	}
	s.Slice3Int64 = slice
	s.Valid = true
	return nil
}

func scanNullFloat64(v interface{}) (sql.NullFloat64, error) {
	if v == nil {
		return sql.NullFloat64{}, nil
	}
	vNumber, ok := v.(json.Number)
	if ok {
		vFloat, err := vNumber.Float64()
		if err != nil {
			return sql.NullFloat64{}, fmt.Errorf("cannot convert %v (%T) to float64", vNumber, vNumber)
		}
		return sql.NullFloat64{Valid: true, Float64: vFloat}, nil
	}
	switch v {
	case "NaN":
		return sql.NullFloat64{Valid: true, Float64: math.NaN()}, nil
	case "Infinity":
		return sql.NullFloat64{Valid: true, Float64: math.Inf(+1)}, nil
	case "-Infinity":
		return sql.NullFloat64{Valid: true, Float64: math.Inf(-1)}, nil
	default:
		return sql.NullFloat64{}, fmt.Errorf("cannot convert %v (%T) to float64", v, v)
	}
}

// NullSliceFloat64 represents a slice of float64 that may be null.
type NullSliceFloat64 struct {
	SliceFloat64 []sql.NullFloat64
	Valid        bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceFloat64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []float64", value, value)
	}
	slice := make([]sql.NullFloat64, len(vs))
	for i := range vs {
		v, err := scanNullFloat64(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceFloat64 = slice
	s.Valid = true
	return nil
}

// NullSlice2Float64 represents a two-dimensional slice of float64 that may be null.
type NullSlice2Float64 struct {
	Slice2Float64 [][]sql.NullFloat64
	Valid         bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Float64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]float64", value, value)
	}
	slice := make([][]sql.NullFloat64, len(vs))
	for i := range vs {
		var ss NullSliceFloat64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceFloat64
	}
	s.Slice2Float64 = slice
	s.Valid = true
	return nil
}

// NullSlice3Float64 represents a three-dimensional slice of float64 that may be null.
type NullSlice3Float64 struct {
	Slice3Float64 [][][]sql.NullFloat64
	Valid         bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Float64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]float64", value, value)
	}
	slice := make([][][]sql.NullFloat64, len(vs))
	for i := range vs {
		var ss NullSlice2Float64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Float64
	}
	s.Slice3Float64 = slice
	s.Valid = true
	return nil
}

var timeLayouts = []string{
	"2006-01-02",
	"15:04:05.000",
	"2006-01-02 15:04:05.000",
}

func scanNullTime(v interface{}) (NullTime, error) {
	if v == nil {
		return NullTime{}, nil
	}
	vv, ok := v.(string)
	if !ok {
		return NullTime{}, fmt.Errorf("cannot convert %v (%T) to time string", v, v)
	}
	vparts := strings.Split(vv, " ")
	if len(vparts) > 1 && !unicode.IsDigit(rune(vparts[len(vparts)-1][0])) {
		return parseNullTimeWithLocation(vv)
	}
	return parseNullTime(vv)
}

func parseNullTime(v string) (NullTime, error) {
	var t time.Time
	var err error
	for _, layout := range timeLayouts {
		t, err = time.ParseInLocation(layout, v, time.Local)
		if err == nil {
			return NullTime{Valid: true, Time: t}, nil
		}
	}
	return NullTime{}, err
}

func parseNullTimeWithLocation(v string) (NullTime, error) {
	idx := strings.LastIndex(v, " ")
	if idx == -1 {
		return NullTime{}, fmt.Errorf("cannot convert %v (%T) to time+zone", v, v)
	}
	stamp, location := v[:idx], v[idx+1:]
	loc, err := time.LoadLocation(location)
	if err != nil {
		return NullTime{}, fmt.Errorf("cannot load timezone %q: %v", location, err)
	}
	var t time.Time
	for _, layout := range timeLayouts {
		t, err = time.ParseInLocation(layout, stamp, loc)
		if err == nil {
			return NullTime{Valid: true, Time: t}, nil
		}
	}
	return NullTime{}, err
}

// NullTime represents a time.Time value that can be null.
// The NullTime supports presto's Date, Time and Timestamp data types,
// with or without time zone.
type NullTime struct {
	Time  time.Time
	Valid bool
}

// Scan implements the sql.Scanner interface.
func (s *NullTime) Scan(value interface{}) error {
	switch value.(type) {
	case time.Time:
		s.Time, s.Valid = value.(time.Time)
	case NullTime:
		*s = value.(NullTime)
	}
	return nil
}

// NullSliceTime represents a slice of time.Time that may be null.
type NullSliceTime struct {
	SliceTime []NullTime
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceTime) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []time.Time", value, value)
	}
	slice := make([]NullTime, len(vs))
	for i := range vs {
		v, err := scanNullTime(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceTime = slice
	s.Valid = true
	return nil
}

// NullSlice2Time represents a two-dimensional slice of time.Time that may be null.
type NullSlice2Time struct {
	Slice2Time [][]NullTime
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Time) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]time.Time", value, value)
	}
	slice := make([][]NullTime, len(vs))
	for i := range vs {
		var ss NullSliceTime
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceTime
	}
	s.Slice2Time = slice
	s.Valid = true
	return nil
}

// NullSlice3Time represents a three-dimensional slice of time.Time that may be null.
type NullSlice3Time struct {
	Slice3Time [][][]NullTime
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Time) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]time.Time", value, value)
	}
	slice := make([][][]NullTime, len(vs))
	for i := range vs {
		var ss NullSlice2Time
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Time
	}
	s.Slice3Time = slice
	s.Valid = true
	return nil
}

// NullMap represents a map type that may be null.
type NullMap struct {
	Map   map[string]interface{}
	Valid bool
}

// Scan implements the sql.Scanner interface.
func (m *NullMap) Scan(v interface{}) error {
	if v == nil {
		return nil
	}
	m.Map, m.Valid = v.(map[string]interface{})
	return nil
}

// NullSliceMap represents a slice of NullMap that may be null.
type NullSliceMap struct {
	SliceMap []NullMap
	Valid    bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceMap) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []NullMap", value, value)
	}
	slice := make([]NullMap, len(vs))
	for i := range vs {
		if err := validateMap(vs[i]); err != nil {
			return fmt.Errorf("cannot convert %v (%T) to []NullMap", value, value)
		}
		m := NullMap{}
		m.Scan(vs[i])
		slice[i] = m
	}
	s.SliceMap = slice
	s.Valid = true
	return nil
}

// NullSlice2Map represents a two-dimensional slice of NullMap that may be null.
type NullSlice2Map struct {
	Slice2Map [][]NullMap
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Map) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]NullMap", value, value)
	}
	slice := make([][]NullMap, len(vs))
	for i := range vs {
		var ss NullSliceMap
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceMap
	}
	s.Slice2Map = slice
	s.Valid = true
	return nil
}

// NullSlice3Map represents a three-dimensional slice of NullMap that may be null.
type NullSlice3Map struct {
	Slice3Map [][][]NullMap
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Map) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]NullMap", value, value)
	}
	slice := make([][][]NullMap, len(vs))
	for i := range vs {
		var ss NullSlice2Map
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Map
	}
	s.Slice3Map = slice
	s.Valid = true
	return nil
}
