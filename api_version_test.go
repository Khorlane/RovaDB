package rovadb

import "testing"

func TestVersionReturnsCurrentProductVersion(t *testing.T) {
	if got := Version(); got != "v0.12.3" {
		t.Fatalf("Version() = %q, want %q", got, "v0.12.3")
	}
}
