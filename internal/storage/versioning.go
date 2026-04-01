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

// FormatSignature is the explicit open-time DB/directory/WAL compatibility signature.
type FormatSignature struct {
	DBFormatVersion        uint32
	DirectoryFormatVersion uint32
	WALVersion             uint32
	WALDBFormatVersion     uint32
	PageSize               uint32
}

// ValidateFormatSignature validates the combined DB, directory, and WAL compatibility contract.
func ValidateFormatSignature(sig FormatSignature) error {
	if !SupportedDBFormatVersion(sig.DBFormatVersion) {
		return errCorruptedDatabaseHeader
	}
	if !SupportedDBFormatVersion(sig.DirectoryFormatVersion) {
		return errCorruptedDirectoryPage
	}
	if sig.DBFormatVersion != sig.DirectoryFormatVersion {
		return errCorruptedDirectoryPage
	}
	if !SupportedWALVersion(sig.WALVersion) {
		return errUnsupportedWALVersion
	}
	if sig.PageSize != PageSize {
		return errWALPageSizeMismatch
	}
	if !CompatibleWALWithDB(sig.WALVersion, sig.WALDBFormatVersion) {
		return errCorruptedWALHeader
	}
	if sig.WALDBFormatVersion != sig.DBFormatVersion {
		return errCorruptedWALHeader
	}
	return nil
}
