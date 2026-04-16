package storage

import (
	"encoding/binary"
	"math"
)

const (
	catalogVersion = 9
	catalogPageID  = 0

	CatalogColumnTypeInt       = 1
	CatalogColumnTypeText      = 2
	CatalogColumnTypeBool      = 3
	CatalogColumnTypeReal      = 4
	CatalogColumnTypeSmallInt  = 5
	CatalogColumnTypeBigInt    = 6
	CatalogColumnTypeDate      = 7
	CatalogColumnTypeTime      = 8
	CatalogColumnTypeTimestamp = 9
)

// CatalogData is the tiny storage-side catalog DTO persisted through CAT/DIR storage.
type CatalogData struct {
	Version              uint32
	DefaultTimezone      string
	TimezoneBasisVersion string
	TimezoneDictionary   []string
	Tables               []CatalogTable
}

// CatalogTable is a persisted table schema entry.
type CatalogTable struct {
	Name        string
	TableID     uint32
	RootPageID  uint32
	RowCount    uint32
	Columns     []CatalogColumn
	Indexes     []CatalogIndex
	PrimaryKey  *CatalogPrimaryKey
	ForeignKeys []CatalogForeignKey
}

// CatalogColumn is a persisted typed column entry.
type CatalogColumn struct {
	Name         string
	Type         uint8
	NotNull      bool
	HasDefault   bool
	DefaultValue Value
}

// CatalogIndex is a persisted index definition.
type CatalogIndex struct {
	Name       string
	Unique     bool
	IndexID    uint32
	RootPageID uint32
	Columns    []CatalogIndexColumn
}

// CatalogIndexColumn is a persisted indexed column entry.
type CatalogIndexColumn struct {
	Name string
	Desc bool
}

type CatalogWritePage struct {
	PageID PageID
	Data   []byte
	IsNew  bool
}

type CatalogWritePlan struct {
	DirectoryPage     []byte
	OverflowPages     []CatalogWritePage
	ReclaimedPages    []CatalogWritePage
	FreeListHead      uint32
	CATDIRStorageMode uint32
}

type CatalogOverflowPageAllocator func() (PageID, bool, error)

// PageReader provides durable page reads without exposing pager internals.
type PageReader interface {
	ReadPage(pageID PageID) ([]byte, error)
}

// PageReaderFunc adapts a function to PageReader.
type PageReaderFunc func(PageID) ([]byte, error)

func (f PageReaderFunc) ReadPage(pageID PageID) ([]byte, error) {
	return f(pageID)
}

// LoadCatalog decodes the committed catalog from embedded or overflow CAT/DIR storage.
func LoadCatalog(reader PageReader) (*CatalogData, error) {
	payload, err := readCommittedCatalogPayload(reader)
	if err != nil {
		return nil, err
	}
	return loadCatalogPayload(payload)
}

// loadCatalogPageData decodes a catalog page image.
func loadCatalogPageData(pageData []byte) (*CatalogData, error) {
	if ValidateDirectoryPage(pageData) == nil {
		payload, err := directoryCatalogPayload(pageData)
		if err != nil {
			return nil, err
		}
		return loadCatalogPayload(payload)
	}
	return loadCatalogPayload(pageData)
}

func readCommittedCatalogPayload(reader PageReader) ([]byte, error) {
	pageData, err := reader.ReadPage(catalogPageID)
	if err != nil {
		return nil, err
	}
	if ValidateDirectoryPage(pageData) != nil {
		return pageData, nil
	}
	return readDirectoryCatalogPayload(reader, pageData)
}

func readDirectoryCatalogPayload(reader PageReader, pageData []byte) ([]byte, error) {
	control, err := directoryCATDIRControl(pageData)
	if err != nil {
		return nil, err
	}
	if control.mode == DirectoryCATDIRStorageModeEmbedded {
		return directoryCatalogPayload(pageData)
	}
	return ReadCatalogOverflowPayload(
		reader,
		PageID(control.overflowHeadPageID),
		control.overflowPageCount,
		control.payloadByteLength,
	)
}

func loadCatalogPayload(pageData []byte) (*CatalogData, error) {
	_, cat, err := decodeCatalogPayload(pageData)
	return cat, err
}

func encodeCatalogPayload(cat *CatalogData) ([]byte, error) {
	if cat == nil {
		cat = &CatalogData{}
	}
	if err := validateCatalogTemporalMetadata(cat.DefaultTimezone, cat.TimezoneBasisVersion, cat.TimezoneDictionary); err != nil {
		return nil, err
	}
	buf := make([]byte, 0, embeddedDirectoryCatalogPayloadCapacity(0))
	buf = appendUint32(buf, catalogVersion)
	buf = appendString(buf, cat.DefaultTimezone)
	buf = appendString(buf, cat.TimezoneBasisVersion)
	buf = appendUint32(buf, uint32(len(cat.TimezoneDictionary)))
	for _, zone := range cat.TimezoneDictionary {
		buf = appendString(buf, zone)
	}
	buf = appendUint32(buf, uint32(len(cat.Tables)))

	for _, table := range cat.Tables {
		if table.Name == "" || len(table.Columns) == 0 {
			return nil, errCorruptedCatalogPage
		}
		buf = appendString(buf, table.Name)
		buf = appendUint32(buf, table.TableID)
		buf = appendUint32(buf, table.RowCount)
		buf = appendUint16(buf, uint16(len(table.Columns)))

		for _, column := range table.Columns {
			if err := validateCatalogColumn(column); err != nil {
				return nil, errCorruptedCatalogPage
			}
			buf = appendString(buf, column.Name)
			buf = append(buf, column.Type)
			var columnFlags byte
			if column.NotNull {
				columnFlags |= 1 << 0
			}
			if column.HasDefault {
				columnFlags |= 1 << 1
			}
			buf = append(buf, columnFlags)
			var err error
			buf, err = appendCatalogDefaultValue(buf, column)
			if err != nil {
				return nil, err
			}
		}
		buf = appendUint16(buf, uint16(len(table.Indexes)))
		for _, index := range table.Indexes {
			var err error
			buf, err = appendCatalogIndex(buf, index, table.Columns)
			if err != nil {
				return nil, err
			}
		}
		var err error
		buf, err = appendCatalogConstraints(buf, table)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func decodeCatalogPayload(pageData []byte) (int, *CatalogData, error) {
	if isZeroPage(pageData) {
		return 0, &CatalogData{}, nil
	}

	offset := 0
	version, ok := readUint32(pageData, &offset)
	if !ok {
		return 0, nil, errCorruptedCatalogPage
	}
	if version != catalogVersion {
		return 0, nil, errUnsupportedCatalogPage
	}
	defaultTimezone, ok := readString(pageData, &offset)
	if !ok {
		return 0, nil, errCorruptedCatalogPage
	}
	timezoneBasisVersion, ok := readString(pageData, &offset)
	if !ok {
		return 0, nil, errCorruptedCatalogPage
	}
	timezoneDictionaryCount, ok := readUint32(pageData, &offset)
	if !ok {
		return 0, nil, errCorruptedCatalogPage
	}
	timezoneDictionary := make([]string, 0, timezoneDictionaryCount)
	for i := uint32(0); i < timezoneDictionaryCount; i++ {
		zone, ok := readString(pageData, &offset)
		if !ok {
			return 0, nil, errCorruptedCatalogPage
		}
		timezoneDictionary = append(timezoneDictionary, zone)
	}
	if err := validateCatalogTemporalMetadata(defaultTimezone, timezoneBasisVersion, timezoneDictionary); err != nil {
		return 0, nil, err
	}
	tableCount, ok := readUint32(pageData, &offset)
	if !ok {
		return 0, nil, errCorruptedCatalogPage
	}

	cat := &CatalogData{
		Version:              version,
		DefaultTimezone:      defaultTimezone,
		TimezoneBasisVersion: timezoneBasisVersion,
		TimezoneDictionary:   append([]string(nil), timezoneDictionary...),
		Tables:               make([]CatalogTable, 0, tableCount),
	}
	for i := uint32(0); i < tableCount; i++ {
		name, ok := readString(pageData, &offset)
		if !ok || name == "" {
			return 0, nil, errCorruptedCatalogPage
		}
		tableID, ok := readUint32(pageData, &offset)
		if !ok || tableID == 0 {
			return 0, nil, errCorruptedCatalogPage
		}
		rowCount, ok := readUint32(pageData, &offset)
		if !ok {
			return 0, nil, errCorruptedCatalogPage
		}
		columnCount, ok := readUint16(pageData, &offset)
		if !ok {
			return 0, nil, errCorruptedCatalogPage
		}

		table := CatalogTable{
			Name:       name,
			TableID:    tableID,
			RootPageID: 0,
			RowCount:   rowCount,
			Columns:    make([]CatalogColumn, 0, columnCount),
		}
		columnNames := make(map[string]struct{}, columnCount)
		for j := uint16(0); j < columnCount; j++ {
			columnName, ok := readString(pageData, &offset)
			if !ok || columnName == "" {
				return 0, nil, errCorruptedCatalogPage
			}
			if offset >= len(pageData) {
				return 0, nil, errCorruptedCatalogPage
			}
			columnType := pageData[offset]
			offset++
			if offset >= len(pageData) {
				return 0, nil, errCorruptedCatalogPage
			}
			columnFlags := pageData[offset]
			offset++
			if _, exists := columnNames[columnName]; exists {
				return 0, nil, errCorruptedCatalogPage
			}
			columnNames[columnName] = struct{}{}

			column := CatalogColumn{
				Name:       columnName,
				Type:       columnType,
				NotNull:    columnFlags&(1<<0) != 0,
				HasDefault: columnFlags&(1<<1) != 0,
			}
			defaultValue, err := readCatalogDefaultValue(pageData, &offset, column)
			if err != nil {
				return 0, nil, err
			}
			column.DefaultValue = defaultValue
			if err := validateCatalogColumn(column); err != nil {
				return 0, nil, err
			}
			table.Columns = append(table.Columns, column)
		}
		indexCount, ok := readUint16(pageData, &offset)
		if !ok {
			return 0, nil, errCorruptedCatalogPage
		}
		table.Indexes = make([]CatalogIndex, 0, indexCount)
		indexNames := make(map[string]struct{}, indexCount)
		for j := uint16(0); j < indexCount; j++ {
			index, err := readCatalogIndex(pageData, &offset, columnNames, indexNames)
			if err != nil {
				return 0, nil, err
			}
			table.Indexes = append(table.Indexes, index)
		}
		if err := readCatalogConstraints(pageData, &offset, &table, columnNames); err != nil {
			return 0, nil, err
		}

		cat.Tables = append(cat.Tables, table)
	}

	return offset, cat, nil
}

func validateCatalogTemporalMetadata(defaultTimezone string, timezoneBasisVersion string, timezoneDictionary []string) error {
	if defaultTimezone == "" && timezoneBasisVersion == "" && len(timezoneDictionary) == 0 {
		return nil
	}
	if defaultTimezone == "" || timezoneBasisVersion == "" || len(timezoneDictionary) == 0 {
		return errCorruptedCatalogPage
	}
	if timezoneDictionary[0] != defaultTimezone {
		return errCorruptedCatalogPage
	}
	seen := make(map[string]struct{}, len(timezoneDictionary))
	for _, zone := range timezoneDictionary {
		if zone == "" {
			return errCorruptedCatalogPage
		}
		if _, exists := seen[zone]; exists {
			return errCorruptedCatalogPage
		}
		seen[zone] = struct{}{}
	}
	return nil
}

// SaveCatalog encodes the catalog into the smallest valid CAT/DIR representation.
func SaveCatalog(pager *Pager, cat *CatalogData) error {
	page, err := pager.Get(catalogPageID)
	if err != nil {
		return err
	}
	freeListHead := uint32(0)
	checkpointMeta := DirectoryCheckpointMetadata{}
	currentMode := DirectoryCATDIRStorageModeEmbedded
	currentOverflowHead := PageID(0)
	currentOverflowCount := uint32(0)
	rootIDMappings := buildDirectoryRootIDMappings(cat)
	if ValidateDirectoryPage(page.Data()) == nil {
		freeListHead, err = DirectoryFreeListHead(page.Data())
		if err != nil {
			return err
		}
		currentMode, err = DirectoryCATDIRStorageMode(page.Data())
		if err != nil {
			return err
		}
		checkpointMeta, err = directoryCheckpointMetadata(page.Data())
		if err != nil {
			return err
		}
		if currentMode == DirectoryCATDIRStorageModeOverflow {
			overflowHead, err := DirectoryCATDIROverflowHeadPageID(page.Data())
			if err != nil {
				return err
			}
			currentOverflowHead = PageID(overflowHead)
			currentOverflowCount, err = DirectoryCATDIROverflowPageCount(page.Data())
			if err != nil {
				return err
			}
		}
		existingMappings, err := directoryRootIDMappings(page.Data())
		if err != nil {
			return err
		}
		rootIDMappings = mergePreservedTableHeaderMappings(cat, rootIDMappings, existingMappings)
	}
	allocator := newCatalogOverflowAllocator(pager, &freeListHead)
	plan, err := PrepareCatalogWritePlanWithRootMappings(cat, rootIDMappings, currentMode, currentOverflowHead, currentOverflowCount, pager, CurrentDBFormatVersion, &freeListHead, checkpointMeta, allocator.Allocate)
	if err != nil {
		return err
	}
	if err := applyCatalogWritePlanToPager(pager, plan); err != nil {
		return err
	}
	// Catalog mutation requires explicit dirty marking; later flush eligibility
	// is driven by dirty tracking rather than implicit full flushes.
	return nil
}

func mergePreservedTableHeaderMappings(cat *CatalogData, rootIDMappings []DirectoryRootIDMapping, existingMappings []DirectoryRootIDMapping) []DirectoryRootIDMapping {
	if len(existingMappings) == 0 {
		return rootIDMappings
	}
	tableIDs := make(map[uint32]struct{}, len(cat.Tables))
	for _, table := range cat.Tables {
		if table.TableID != 0 {
			tableIDs[table.TableID] = struct{}{}
		}
	}
	merged := append([]DirectoryRootIDMapping(nil), rootIDMappings...)
	for _, mapping := range existingMappings {
		if mapping.ObjectType != DirectoryRootMappingObjectTableHeader {
			continue
		}
		if _, ok := tableIDs[mapping.ObjectID]; !ok {
			continue
		}
		merged = append(merged, mapping)
	}
	return merged
}

// BuildCatalogPageData encodes the catalog into a full embedded directory page image.
func BuildCatalogPageData(cat *CatalogData) ([]byte, error) {
	return buildCatalogPageDataWithDirectoryState(cat, 0, DirectoryCheckpointMetadata{})
}

// buildCatalogPageDataWithDirectoryState encodes the wrapped page 0 image with
// directory state. It stays storage-private because only the fully embedded
// image is a useful boundary contract.
func buildCatalogPageDataWithDirectoryState(cat *CatalogData, freeListHead uint32, checkpointMeta DirectoryCheckpointMetadata) ([]byte, error) {
	plan, err := prepareCatalogWritePlan(cat, DirectoryCATDIRStorageModeEmbedded, 0, 0, nil, CurrentDBFormatVersion, &freeListHead, checkpointMeta, nil)
	if err != nil {
		return nil, err
	}
	return plan.DirectoryPage, nil
}

func isZeroPage(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

func appendUint32(buf []byte, value uint32) []byte {
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], value)
	return append(buf, raw[:]...)
}

func appendUint16(buf []byte, value uint16) []byte {
	var raw [2]byte
	binary.LittleEndian.PutUint16(raw[:], value)
	return append(buf, raw[:]...)
}

func appendString(buf []byte, value string) []byte {
	buf = appendUint16(buf, uint16(len(value)))
	return append(buf, value...)
}

func readUint32(data []byte, offset *int) (uint32, bool) {
	if *offset+4 > len(data) {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(data[*offset : *offset+4])
	*offset += 4
	return value, true
}

func readUint16(data []byte, offset *int) (uint16, bool) {
	if *offset+2 > len(data) {
		return 0, false
	}
	value := binary.LittleEndian.Uint16(data[*offset : *offset+2])
	*offset += 2
	return value, true
}

func readString(data []byte, offset *int) (string, bool) {
	length, ok := readUint16(data, offset)
	if !ok {
		return "", false
	}
	if *offset+int(length) > len(data) {
		return "", false
	}
	value := string(data[*offset : *offset+int(length)])
	*offset += int(length)
	return value, true
}

func validateCatalogColumn(column CatalogColumn) error {
	if column.Name == "" {
		return errCorruptedCatalogPage
	}
	if !catalogColumnTypeValid(column.Type) {
		return errCorruptedCatalogPage
	}
	if !column.HasDefault {
		if column.DefaultValue.Kind != ValueKindInvalid {
			return errCorruptedCatalogPage
		}
		return nil
	}
	if !catalogDefaultValueMatchesColumnType(column.Type, column.DefaultValue) {
		return errCorruptedCatalogPage
	}
	if column.NotNull && column.DefaultValue.Kind == ValueKindNull {
		return errCorruptedCatalogPage
	}
	return nil
}

func appendCatalogDefaultValue(buf []byte, column CatalogColumn) ([]byte, error) {
	if !column.HasDefault {
		return buf, nil
	}
	switch column.DefaultValue.Kind {
	case ValueKindNull:
		return append(buf, rowTypeNull), nil
	case ValueKindIntegerLiteral, ValueKindSmallInt, ValueKindInt, ValueKindBigInt:
		return appendCatalogIntegerDefaultValue(buf, column)
	case ValueKindString:
		var raw [4]byte
		buf = append(buf, rowTypeString)
		binary.LittleEndian.PutUint32(raw[:], uint32(len(column.DefaultValue.Str)))
		buf = append(buf, raw[:]...)
		return append(buf, column.DefaultValue.Str...), nil
	case ValueKindBool:
		buf = append(buf, rowTypeBool)
		if column.DefaultValue.Bool {
			return append(buf, 1), nil
		}
		return append(buf, 0), nil
	case ValueKindReal:
		var raw [8]byte
		buf = append(buf, rowTypeReal)
		binary.LittleEndian.PutUint64(raw[:], math.Float64bits(column.DefaultValue.F64))
		return append(buf, raw[:]...), nil
	case ValueKindDate:
		var raw [4]byte
		buf = append(buf, rowTypeDate)
		binary.LittleEndian.PutUint32(raw[:], uint32(column.DefaultValue.DateDays))
		return append(buf, raw[:]...), nil
	case ValueKindTime:
		if !validTimeSeconds(column.DefaultValue.TimeSeconds) {
			return nil, errCorruptedCatalogPage
		}
		var raw [4]byte
		buf = append(buf, rowTypeTime)
		binary.LittleEndian.PutUint32(raw[:], uint32(column.DefaultValue.TimeSeconds))
		return append(buf, raw[:]...), nil
	case ValueKindTimestamp:
		var millisRaw [8]byte
		var zoneRaw [2]byte
		buf = append(buf, rowTypeTimestamp)
		binary.LittleEndian.PutUint64(millisRaw[:], uint64(column.DefaultValue.TimestampMillis))
		binary.LittleEndian.PutUint16(zoneRaw[:], uint16(column.DefaultValue.TimestampZoneID))
		buf = append(buf, millisRaw[:]...)
		return append(buf, zoneRaw[:]...), nil
	default:
		return nil, errCorruptedCatalogPage
	}
}

func readCatalogDefaultValue(data []byte, offset *int, column CatalogColumn) (Value, error) {
	if !column.HasDefault {
		return Value{}, nil
	}
	if *offset >= len(data) {
		return Value{}, errCorruptedCatalogPage
	}
	tag := data[*offset]
	*offset++
	switch tag {
	case rowTypeNull:
		return NullValue(), nil
	case rowTypeInt64:
		if *offset+8 > len(data) {
			return Value{}, errCorruptedCatalogPage
		}
		value := int64(binary.LittleEndian.Uint64(data[*offset : *offset+8]))
		*offset += 8
		return catalogIntegerValueForColumn(column.Type, value)
	case rowTypeString:
		if *offset+4 > len(data) {
			return Value{}, errCorruptedCatalogPage
		}
		length := int(binary.LittleEndian.Uint32(data[*offset : *offset+4]))
		*offset += 4
		if *offset+length > len(data) {
			return Value{}, errCorruptedCatalogPage
		}
		value := StringValue(string(data[*offset : *offset+length]))
		*offset += length
		return value, nil
	case rowTypeBool:
		if *offset >= len(data) {
			return Value{}, errCorruptedCatalogPage
		}
		switch data[*offset] {
		case 0:
			*offset++
			return BoolValue(false), nil
		case 1:
			*offset++
			return BoolValue(true), nil
		default:
			return Value{}, errCorruptedCatalogPage
		}
	case rowTypeReal:
		if *offset+8 > len(data) {
			return Value{}, errCorruptedCatalogPage
		}
		value := RealValue(math.Float64frombits(binary.LittleEndian.Uint64(data[*offset : *offset+8])))
		*offset += 8
		return value, nil
	case rowTypeDate:
		if *offset+4 > len(data) {
			return Value{}, errCorruptedCatalogPage
		}
		value := DateValue(int32(binary.LittleEndian.Uint32(data[*offset : *offset+4])))
		*offset += 4
		return value, nil
	case rowTypeTime:
		if *offset+4 > len(data) {
			return Value{}, errCorruptedCatalogPage
		}
		value := int32(binary.LittleEndian.Uint32(data[*offset : *offset+4]))
		*offset += 4
		if !validTimeSeconds(value) {
			return Value{}, errCorruptedCatalogPage
		}
		return TimeValue(value), nil
	case rowTypeTimestamp:
		if *offset+10 > len(data) {
			return Value{}, errCorruptedCatalogPage
		}
		millis := int64(binary.LittleEndian.Uint64(data[*offset : *offset+8]))
		*offset += 8
		zoneID := int16(binary.LittleEndian.Uint16(data[*offset : *offset+2]))
		*offset += 2
		return TimestampValue(millis, zoneID), nil
	default:
		return Value{}, errCorruptedCatalogPage
	}
}

func catalogDefaultValueMatchesColumnType(columnType uint8, value Value) bool {
	switch value.Kind {
	case ValueKindNull:
		return true
	case ValueKindSmallInt:
		return columnType == CatalogColumnTypeSmallInt
	case ValueKindInt:
		return columnType == CatalogColumnTypeInt
	case ValueKindBigInt:
		return columnType == CatalogColumnTypeBigInt
	case ValueKindIntegerLiteral:
		return false
	case ValueKindString:
		return columnType == CatalogColumnTypeText
	case ValueKindBool:
		return columnType == CatalogColumnTypeBool
	case ValueKindReal:
		return columnType == CatalogColumnTypeReal
	case ValueKindDate:
		return columnType == CatalogColumnTypeDate
	case ValueKindTime:
		return columnType == CatalogColumnTypeTime
	case ValueKindTimestamp:
		return columnType == CatalogColumnTypeTimestamp
	default:
		return false
	}
}

func appendCatalogIntegerDefaultValue(buf []byte, column CatalogColumn) ([]byte, error) {
	integerValue := column.DefaultValue.IntegerValue()
	switch column.Type {
	case CatalogColumnTypeSmallInt:
		if integerValue < math.MinInt16 || integerValue > math.MaxInt16 {
			return nil, errCorruptedCatalogPage
		}
	case CatalogColumnTypeInt:
		if !publicIntInRange(integerValue) {
			return nil, errCorruptedCatalogPage
		}
	case CatalogColumnTypeBigInt:
	default:
		if !publicIntInRange(integerValue) {
			return nil, errCorruptedCatalogPage
		}
	}
	var raw [8]byte
	buf = append(buf, rowTypeInt64)
	binary.LittleEndian.PutUint64(raw[:], uint64(integerValue))
	return append(buf, raw[:]...), nil
}

func catalogIntegerValueForColumn(columnType uint8, value int64) (Value, error) {
	switch columnType {
	case CatalogColumnTypeSmallInt:
		if value < math.MinInt16 || value > math.MaxInt16 {
			return Value{}, errCorruptedCatalogPage
		}
		return SmallIntValue(int16(value)), nil
	case CatalogColumnTypeInt:
		if !publicIntInRange(value) {
			return Value{}, errCorruptedCatalogPage
		}
		return IntValue(int32(value)), nil
	case CatalogColumnTypeBigInt:
		return BigIntValue(value), nil
	default:
		if !publicIntInRange(value) {
			return Value{}, errCorruptedCatalogPage
		}
		return IntegerLiteralValue(value), nil
	}
}

func catalogColumnTypeValid(columnType uint8) bool {
	switch columnType {
	case CatalogColumnTypeSmallInt, CatalogColumnTypeInt, CatalogColumnTypeBigInt, CatalogColumnTypeText, CatalogColumnTypeBool, CatalogColumnTypeReal, CatalogColumnTypeDate, CatalogColumnTypeTime, CatalogColumnTypeTimestamp:
		return true
	default:
		return false
	}
}

func catalogColumnTypeIsInteger(columnType uint8) bool {
	switch columnType {
	case CatalogColumnTypeSmallInt, CatalogColumnTypeInt, CatalogColumnTypeBigInt:
		return true
	default:
		return false
	}
}

func readCatalogIndex(data []byte, offset *int, columnNames map[string]struct{}, indexNames map[string]struct{}) (CatalogIndex, error) {
	name, ok := readString(data, offset)
	if !ok || name == "" {
		return CatalogIndex{}, errCorruptedCatalogPage
	}
	if _, exists := indexNames[name]; exists {
		return CatalogIndex{}, errCorruptedIndexMetadata
	}
	if *offset >= len(data) {
		return CatalogIndex{}, errCorruptedCatalogPage
	}
	unique := data[*offset] != 0
	*offset++
	indexID, ok := readUint32(data, offset)
	if !ok || indexID == 0 {
		return CatalogIndex{}, errCorruptedCatalogPage
	}
	columnCount, ok := readUint16(data, offset)
	if !ok || columnCount == 0 {
		return CatalogIndex{}, errCorruptedIndexMetadata
	}
	columns := make([]CatalogIndexColumn, 0, columnCount)
	seenColumns := make(map[string]struct{}, columnCount)
	for i := uint16(0); i < columnCount; i++ {
		columnName, ok := readString(data, offset)
		if !ok || columnName == "" {
			return CatalogIndex{}, errCorruptedCatalogPage
		}
		if _, exists := columnNames[columnName]; !exists {
			return CatalogIndex{}, errCorruptedIndexMetadata
		}
		if _, exists := seenColumns[columnName]; exists {
			return CatalogIndex{}, errCorruptedIndexMetadata
		}
		seenColumns[columnName] = struct{}{}
		if *offset >= len(data) {
			return CatalogIndex{}, errCorruptedCatalogPage
		}
		desc := data[*offset] != 0
		*offset++
		columns = append(columns, CatalogIndexColumn{Name: columnName, Desc: desc})
	}
	indexNames[name] = struct{}{}
	return CatalogIndex{Name: name, Unique: unique, IndexID: indexID, RootPageID: 0, Columns: columns}, nil
}

func appendCatalogIndex(buf []byte, index CatalogIndex, columns []CatalogColumn) ([]byte, error) {
	if index.Name == "" || len(index.Columns) == 0 {
		return nil, errCorruptedIndexMetadata
	}
	validColumns := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		validColumns[column.Name] = struct{}{}
	}
	seenColumns := make(map[string]struct{}, len(index.Columns))

	buf = appendString(buf, index.Name)
	if index.Unique {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = appendUint32(buf, index.IndexID)
	buf = appendUint16(buf, uint16(len(index.Columns)))
	for _, column := range index.Columns {
		if column.Name == "" {
			return nil, errCorruptedIndexMetadata
		}
		if _, exists := validColumns[column.Name]; !exists {
			return nil, errCorruptedIndexMetadata
		}
		if _, exists := seenColumns[column.Name]; exists {
			return nil, errCorruptedIndexMetadata
		}
		seenColumns[column.Name] = struct{}{}
		buf = appendString(buf, column.Name)
		if column.Desc {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	return buf, nil
}

func appendCatalogConstraints(buf []byte, table CatalogTable) ([]byte, error) {
	validColumns := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		validColumns[column.Name] = struct{}{}
	}
	constraintNames := make(map[string]struct{}, 1+len(table.ForeignKeys))
	if table.PrimaryKey != nil {
		if table.PrimaryKey.Name == "" || table.PrimaryKey.TableID == 0 || table.PrimaryKey.TableID != table.TableID || len(table.PrimaryKey.Columns) == 0 || table.PrimaryKey.IndexID == 0 {
			return nil, errCorruptedCatalogPage
		}
		if _, exists := constraintNames[table.PrimaryKey.Name]; exists {
			return nil, errCorruptedCatalogPage
		}
		constraintNames[table.PrimaryKey.Name] = struct{}{}
		buf = append(buf, 1)
		buf = appendString(buf, table.PrimaryKey.Name)
		buf = appendUint32(buf, table.PrimaryKey.TableID)
		buf = appendUint16(buf, uint16(len(table.PrimaryKey.Columns)))
		seenColumns := make(map[string]struct{}, len(table.PrimaryKey.Columns))
		for _, columnName := range table.PrimaryKey.Columns {
			if columnName == "" {
				return nil, errCorruptedCatalogPage
			}
			if _, ok := validColumns[columnName]; !ok {
				return nil, errCorruptedCatalogPage
			}
			if _, exists := seenColumns[columnName]; exists {
				return nil, errCorruptedCatalogPage
			}
			seenColumns[columnName] = struct{}{}
			buf = appendString(buf, columnName)
		}
		buf = appendUint32(buf, table.PrimaryKey.IndexID)
		if table.PrimaryKey.ImplicitNN {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	} else {
		buf = append(buf, 0)
	}
	buf = appendUint16(buf, uint16(len(table.ForeignKeys)))
	for _, fk := range table.ForeignKeys {
		if fk.Name == "" || fk.ChildTableID == 0 || fk.ChildTableID != table.TableID || len(fk.ChildColumns) == 0 || fk.ParentTableID == 0 || len(fk.ParentColumns) == 0 || fk.ParentPrimaryKeyName == "" || fk.ChildIndexID == 0 {
			return nil, errCorruptedCatalogPage
		}
		if fk.OnDeleteAction != CatalogForeignKeyDeleteActionRestrict && fk.OnDeleteAction != CatalogForeignKeyDeleteActionCascade {
			return nil, errCorruptedCatalogPage
		}
		if _, exists := constraintNames[fk.Name]; exists {
			return nil, errCorruptedCatalogPage
		}
		constraintNames[fk.Name] = struct{}{}
		buf = appendString(buf, fk.Name)
		buf = appendUint32(buf, fk.ChildTableID)
		buf = appendUint16(buf, uint16(len(fk.ChildColumns)))
		seenChildColumns := make(map[string]struct{}, len(fk.ChildColumns))
		for _, columnName := range fk.ChildColumns {
			if columnName == "" {
				return nil, errCorruptedCatalogPage
			}
			if _, ok := validColumns[columnName]; !ok {
				return nil, errCorruptedCatalogPage
			}
			if _, exists := seenChildColumns[columnName]; exists {
				return nil, errCorruptedCatalogPage
			}
			seenChildColumns[columnName] = struct{}{}
			buf = appendString(buf, columnName)
		}
		buf = appendUint32(buf, fk.ParentTableID)
		buf = appendUint16(buf, uint16(len(fk.ParentColumns)))
		seenParentColumns := make(map[string]struct{}, len(fk.ParentColumns))
		for _, columnName := range fk.ParentColumns {
			if columnName == "" {
				return nil, errCorruptedCatalogPage
			}
			if _, exists := seenParentColumns[columnName]; exists {
				return nil, errCorruptedCatalogPage
			}
			seenParentColumns[columnName] = struct{}{}
			buf = appendString(buf, columnName)
		}
		buf = appendString(buf, fk.ParentPrimaryKeyName)
		buf = appendUint32(buf, fk.ChildIndexID)
		buf = append(buf, fk.OnDeleteAction)
	}
	return buf, nil
}

func readCatalogConstraints(data []byte, offset *int, table *CatalogTable, columnNames map[string]struct{}) error {
	if offset == nil || table == nil {
		return errCorruptedCatalogPage
	}
	if *offset >= len(data) {
		return errCorruptedCatalogPage
	}
	hasPrimaryKey := data[*offset]
	*offset++
	if hasPrimaryKey > 1 {
		return errCorruptedCatalogPage
	}
	constraintNames := make(map[string]struct{}, 1)
	indexIDs := make(map[uint32]struct{}, len(table.Indexes))
	for _, index := range table.Indexes {
		indexIDs[index.IndexID] = struct{}{}
	}
	if hasPrimaryKey == 1 {
		pk, err := readCatalogPrimaryKey(data, offset, table.TableID, columnNames, indexIDs)
		if err != nil {
			return err
		}
		table.PrimaryKey = &pk
		constraintNames[pk.Name] = struct{}{}
	}
	fkCount, ok := readUint16(data, offset)
	if !ok {
		return errCorruptedCatalogPage
	}
	table.ForeignKeys = make([]CatalogForeignKey, 0, fkCount)
	for i := uint16(0); i < fkCount; i++ {
		fk, err := readCatalogForeignKey(data, offset, table.TableID, columnNames, indexIDs)
		if err != nil {
			return err
		}
		if _, exists := constraintNames[fk.Name]; exists {
			return errCorruptedCatalogPage
		}
		constraintNames[fk.Name] = struct{}{}
		table.ForeignKeys = append(table.ForeignKeys, fk)
	}
	return nil
}

func readCatalogPrimaryKey(data []byte, offset *int, tableID uint32, columnNames map[string]struct{}, indexIDs map[uint32]struct{}) (CatalogPrimaryKey, error) {
	name, ok := readString(data, offset)
	if !ok || name == "" {
		return CatalogPrimaryKey{}, errCorruptedCatalogPage
	}
	owningTableID, ok := readUint32(data, offset)
	if !ok || owningTableID == 0 || owningTableID != tableID {
		return CatalogPrimaryKey{}, errCorruptedCatalogPage
	}
	columnCount, ok := readUint16(data, offset)
	if !ok || columnCount == 0 {
		return CatalogPrimaryKey{}, errCorruptedCatalogPage
	}
	columns := make([]string, 0, columnCount)
	seenColumns := make(map[string]struct{}, columnCount)
	for i := uint16(0); i < columnCount; i++ {
		columnName, ok := readString(data, offset)
		if !ok || columnName == "" {
			return CatalogPrimaryKey{}, errCorruptedCatalogPage
		}
		if _, exists := columnNames[columnName]; !exists {
			return CatalogPrimaryKey{}, errCorruptedCatalogPage
		}
		if _, exists := seenColumns[columnName]; exists {
			return CatalogPrimaryKey{}, errCorruptedCatalogPage
		}
		seenColumns[columnName] = struct{}{}
		columns = append(columns, columnName)
	}
	indexID, ok := readUint32(data, offset)
	if !ok || indexID == 0 {
		return CatalogPrimaryKey{}, errCorruptedCatalogPage
	}
	if _, exists := indexIDs[indexID]; !exists {
		return CatalogPrimaryKey{}, errCorruptedCatalogPage
	}
	if *offset >= len(data) {
		return CatalogPrimaryKey{}, errCorruptedCatalogPage
	}
	implicitNN := data[*offset] != 0
	*offset++
	return CatalogPrimaryKey{
		Name:       name,
		TableID:    owningTableID,
		Columns:    columns,
		IndexID:    indexID,
		ImplicitNN: implicitNN,
	}, nil
}

func readCatalogForeignKey(data []byte, offset *int, tableID uint32, columnNames map[string]struct{}, indexIDs map[uint32]struct{}) (CatalogForeignKey, error) {
	name, ok := readString(data, offset)
	if !ok || name == "" {
		return CatalogForeignKey{}, errCorruptedCatalogPage
	}
	childTableID, ok := readUint32(data, offset)
	if !ok || childTableID == 0 || childTableID != tableID {
		return CatalogForeignKey{}, errCorruptedCatalogPage
	}
	childColumnCount, ok := readUint16(data, offset)
	if !ok || childColumnCount == 0 {
		return CatalogForeignKey{}, errCorruptedCatalogPage
	}
	childColumns := make([]string, 0, childColumnCount)
	seenChildColumns := make(map[string]struct{}, childColumnCount)
	for i := uint16(0); i < childColumnCount; i++ {
		columnName, ok := readString(data, offset)
		if !ok || columnName == "" {
			return CatalogForeignKey{}, errCorruptedCatalogPage
		}
		if _, exists := columnNames[columnName]; !exists {
			return CatalogForeignKey{}, errCorruptedCatalogPage
		}
		if _, exists := seenChildColumns[columnName]; exists {
			return CatalogForeignKey{}, errCorruptedCatalogPage
		}
		seenChildColumns[columnName] = struct{}{}
		childColumns = append(childColumns, columnName)
	}
	parentTableID, ok := readUint32(data, offset)
	if !ok || parentTableID == 0 {
		return CatalogForeignKey{}, errCorruptedCatalogPage
	}
	parentColumnCount, ok := readUint16(data, offset)
	if !ok || parentColumnCount == 0 {
		return CatalogForeignKey{}, errCorruptedCatalogPage
	}
	parentColumns := make([]string, 0, parentColumnCount)
	seenParentColumns := make(map[string]struct{}, parentColumnCount)
	for i := uint16(0); i < parentColumnCount; i++ {
		columnName, ok := readString(data, offset)
		if !ok || columnName == "" {
			return CatalogForeignKey{}, errCorruptedCatalogPage
		}
		if _, exists := seenParentColumns[columnName]; exists {
			return CatalogForeignKey{}, errCorruptedCatalogPage
		}
		seenParentColumns[columnName] = struct{}{}
		parentColumns = append(parentColumns, columnName)
	}
	parentPrimaryKeyName, ok := readString(data, offset)
	if !ok || parentPrimaryKeyName == "" {
		return CatalogForeignKey{}, errCorruptedCatalogPage
	}
	childIndexID, ok := readUint32(data, offset)
	if !ok || childIndexID == 0 {
		return CatalogForeignKey{}, errCorruptedCatalogPage
	}
	if _, exists := indexIDs[childIndexID]; !exists {
		return CatalogForeignKey{}, errCorruptedCatalogPage
	}
	if *offset >= len(data) {
		return CatalogForeignKey{}, errCorruptedCatalogPage
	}
	onDeleteAction := data[*offset]
	*offset++
	if onDeleteAction != CatalogForeignKeyDeleteActionRestrict && onDeleteAction != CatalogForeignKeyDeleteActionCascade {
		return CatalogForeignKey{}, errCorruptedCatalogPage
	}
	return CatalogForeignKey{
		Name:                 name,
		ChildTableID:         childTableID,
		ChildColumns:         childColumns,
		ParentTableID:        parentTableID,
		ParentColumns:        parentColumns,
		ParentPrimaryKeyName: parentPrimaryKeyName,
		ChildIndexID:         childIndexID,
		OnDeleteAction:       onDeleteAction,
	}, nil
}

func prepareCatalogWritePlan(cat *CatalogData, currentMode uint32, currentOverflowHead PageID, currentOverflowPageCount uint32, reader PageReader, formatVersion uint32, freeListHead *uint32, checkpointMeta DirectoryCheckpointMetadata, allocate CatalogOverflowPageAllocator) (*CatalogWritePlan, error) {
	return PrepareCatalogWritePlanWithRootMappings(cat, buildDirectoryRootIDMappings(cat), currentMode, currentOverflowHead, currentOverflowPageCount, reader, formatVersion, freeListHead, checkpointMeta, allocate)
}

func PrepareCatalogWritePlanWithRootMappings(cat *CatalogData, rootIDMappings []DirectoryRootIDMapping, currentMode uint32, currentOverflowHead PageID, currentOverflowPageCount uint32, reader PageReader, formatVersion uint32, freeListHead *uint32, checkpointMeta DirectoryCheckpointMetadata, allocate CatalogOverflowPageAllocator) (*CatalogWritePlan, error) {
	if freeListHead == nil {
		return nil, errCorruptedDirectoryPage
	}
	rootIDPayload, err := encodeDirectoryRootIDMappings(rootIDMappings)
	if err != nil {
		return nil, err
	}
	payload, err := encodeCatalogPayload(cat)
	if err != nil {
		return nil, err
	}

	newMode := DirectoryCATDIRStorageModeEmbedded
	var pageAlloc []CatalogWritePage
	newOverflowHead := PageID(0)
	newOverflowCount := uint32(0)
	if !canStoreCatalogPayloadEmbedded(payload, rootIDPayload) {
		if allocate == nil {
			return nil, errCATDIRExceedsEmbeddedWrite
		}
		requiredPages := catalogOverflowRequiredPageCount(len(payload))
		pageIDs := make([]PageID, 0, requiredPages)
		pageAlloc = make([]CatalogWritePage, 0, requiredPages)
		for i := 0; i < requiredPages; i++ {
			pageID, isNew, err := allocate()
			if err != nil {
				return nil, err
			}
			pageIDs = append(pageIDs, pageID)
			pageAlloc = append(pageAlloc, CatalogWritePage{PageID: pageID, IsNew: isNew})
		}
		overflowImages, err := BuildCatalogOverflowPageChain(payload, pageIDs)
		if err != nil {
			return nil, err
		}
		for i := range overflowImages {
			pageAlloc[i].Data = overflowImages[i].Data
		}
		newMode = DirectoryCATDIRStorageModeOverflow
		newOverflowHead = overflowImages[0].PageID
		newOverflowCount = uint32(len(overflowImages))
	}

	var reclaimedPages []CatalogWritePage
	if currentMode == DirectoryCATDIRStorageModeOverflow {
		if currentOverflowHead == 0 || currentOverflowPageCount == 0 || reader == nil {
			return nil, errCorruptedCatalogOverflow
		}
		reclaimedPages, *freeListHead, err = buildCatalogOverflowReclaimPages(reader, currentOverflowHead, currentOverflowPageCount, *freeListHead)
		if err != nil {
			return nil, err
		}
	}

	var directoryPage []byte
	if newMode == DirectoryCATDIRStorageModeEmbedded {
		directoryPage, err = buildEmbeddedDirectoryCatalogPage(payload, formatVersion, *freeListHead, rootIDMappings, checkpointMeta)
		if err != nil {
			return nil, err
		}
	} else {
		directoryPage, err = buildOverflowDirectoryCatalogPage(uint32(len(payload)), newOverflowHead, newOverflowCount, formatVersion, *freeListHead, rootIDMappings, checkpointMeta)
		if err != nil {
			return nil, err
		}
	}
	return &CatalogWritePlan{
		DirectoryPage:     directoryPage,
		OverflowPages:     pageAlloc,
		ReclaimedPages:    reclaimedPages,
		FreeListHead:      *freeListHead,
		CATDIRStorageMode: newMode,
	}, nil
}

type catalogOverflowPagerAllocator struct {
	pager        *Pager
	nextFreshID  PageID
	freeListHead *uint32
}

var newCatalogOverflowAllocator = newCatalogOverflowPagerAllocator

func newCatalogOverflowPagerAllocator(pager *Pager, freeListHead *uint32) *catalogOverflowPagerAllocator {
	return &catalogOverflowPagerAllocator{
		pager:        pager,
		nextFreshID:  pager.NextPageID(),
		freeListHead: freeListHead,
	}
}

func (a *catalogOverflowPagerAllocator) Allocate() (PageID, bool, error) {
	if a == nil || a.pager == nil || a.freeListHead == nil {
		return 0, false, errCorruptedDirectoryPage
	}
	allocator := PageAllocator{
		NextPageID: uint32(a.nextFreshID),
		FreePage: FreePageState{
			HeadPageID: *a.freeListHead,
		},
		ReadFreeNext: func(pageID uint32) (uint32, error) {
			pageData, err := a.pager.ReadPage(PageID(pageID))
			if err != nil {
				return 0, err
			}
			return FreePageNext(pageData)
		},
	}
	allocated, reused, err := allocator.Allocate()
	if err != nil {
		return 0, false, err
	}
	a.nextFreshID = PageID(allocator.NextPageID)
	*a.freeListHead = allocator.FreePage.HeadPageID
	return PageID(allocated), !reused, nil
}

func applyCatalogWritePlanToPager(pager *Pager, plan *CatalogWritePlan) error {
	if pager == nil || plan == nil {
		return errCorruptedDirectoryPage
	}
	for _, overflowPage := range plan.OverflowPages {
		var page *Page
		var err error
		if overflowPage.IsNew {
			page = pager.NewPage()
			if page.ID() != overflowPage.PageID {
				pager.DiscardNewPage(page.ID())
				return errCorruptedCatalogOverflow
			}
		} else {
			page, err = pager.Get(overflowPage.PageID)
			if err != nil {
				return err
			}
		}
		pager.MarkDirtyWithOriginal(page)
		clear(page.data)
		copy(page.data, overflowPage.Data)
	}
	for _, reclaimedPage := range plan.ReclaimedPages {
		page, err := pager.Get(reclaimedPage.PageID)
		if err != nil {
			return err
		}
		pager.MarkDirtyWithOriginal(page)
		clear(page.data)
		copy(page.data, reclaimedPage.Data)
	}
	page, err := pager.Get(catalogPageID)
	if err != nil {
		return err
	}
	pager.MarkDirtyWithOriginal(page)
	clear(page.data)
	copy(page.data, plan.DirectoryPage)
	return nil
}
