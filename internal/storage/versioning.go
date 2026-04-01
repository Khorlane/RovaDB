package storage

// CurrentDBFormatVersion is the authoritative durable database format version.
const CurrentDBFormatVersion uint32 = 1

// CurrentWALVersion is the authoritative WAL sidecar format version.
const CurrentWALVersion uint32 = 1

// DBFormatVersion reports the current durable database file format version.
func DBFormatVersion() uint32 {
	return CurrentDBFormatVersion
}

// SupportedDBFormatVersion reports whether the durable DB format version is supported.
func SupportedDBFormatVersion(v uint32) bool {
	return v == CurrentDBFormatVersion
}

// SupportedWALVersion reports whether the WAL format version is supported.
func SupportedWALVersion(v uint32) bool {
	return v == CurrentWALVersion
}

// CompatibleWALWithDB reports whether the WAL header versions are compatible with the DB format.
func CompatibleWALWithDB(walVersion uint32, dbFormatVersion uint32) bool {
	return SupportedWALVersion(walVersion) && SupportedDBFormatVersion(dbFormatVersion)
}
