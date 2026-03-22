package rovadb

// DB is the top-level handle for a RovaDB database.
type DB struct{}

// Open returns a database handle for the given path.
func Open(path string) (*DB, error) {
	return &DB{}, nil
}

// Close releases database resources.
func (db *DB) Close() error {
	return nil
}
