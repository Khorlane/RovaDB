package rovadb

import "path/filepath"
import "testing"

func testDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.db")
}
