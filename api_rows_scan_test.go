package rovadb

import (
	"errors"
	"testing"
)

func TestRowsScanHappyPath(t *testing.T) {
	rows := newRows(nil, [][]any{{1, "a"}})
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var i int
	var s string
	if err := rows.Scan(&i, &s); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if i != 1 || s != "a" {
		t.Fatalf("Scan() = (%d, %q), want (1, %q)", i, s, "a")
	}
}

func TestRowsScanExactDestinationCountRequired(t *testing.T) {
	rows := newRows(nil, [][]any{{1, "a"}})
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var i int
	if err := rows.Scan(&i); !errors.Is(err, ErrScanMismatch) {
		t.Fatalf("Scan() with too few dests = %v, want ErrScanMismatch", err)
	}

	var s string
	var extra any
	if err := rows.Scan(&i, &s, &extra); !errors.Is(err, ErrScanMismatch) {
		t.Fatalf("Scan() with too many dests = %v, want ErrScanMismatch", err)
	}
}

func TestRowsScanRequiresNext(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})

	var i int
	if err := rows.Scan(&i); !errors.Is(err, ErrScanBeforeNext) {
		t.Fatalf("Scan() before Next = %v, want ErrScanBeforeNext", err)
	}
}

func TestRowsScanAfterExhaustion(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})
	if !rows.Next() {
		t.Fatal("first Next() = false, want true")
	}
	if rows.Next() {
		t.Fatal("second Next() = true, want false")
	}

	var i int
	if err := rows.Scan(&i); !errors.Is(err, ErrScanBeforeNext) {
		t.Fatalf("Scan() after exhaustion = %v, want ErrScanBeforeNext", err)
	}
}

func TestRowsScanTypeMismatchCases(t *testing.T) {
	t.Run("string into int", func(t *testing.T) {
		rows := newRows(nil, [][]any{{"a"}})
		rows.Next()
		var i int
		if err := rows.Scan(&i); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})

	t.Run("int into string", func(t *testing.T) {
		rows := newRows(nil, [][]any{{1}})
		rows.Next()
		var s string
		if err := rows.Scan(&s); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})

	t.Run("nil into int", func(t *testing.T) {
		rows := newRows(nil, [][]any{{nil}})
		rows.Next()
		var i int
		if err := rows.Scan(&i); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})

	t.Run("nil into string", func(t *testing.T) {
		rows := newRows(nil, [][]any{{nil}})
		rows.Next()
		var s string
		if err := rows.Scan(&s); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})

	t.Run("real into int", func(t *testing.T) {
		rows := newRows(nil, [][]any{{3.14}})
		rows.Next()
		var i int
		if err := rows.Scan(&i); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})

	t.Run("int into real", func(t *testing.T) {
		rows := newRows(nil, [][]any{{1}})
		rows.Next()
		var f float64
		if err := rows.Scan(&f); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})
}

func TestRowsScanAnySupport(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		rows := newRows(nil, [][]any{{1}})
		rows.Next()
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if got != 1 {
			t.Fatalf("Scan() got %#v, want 1", got)
		}
	})

	t.Run("string", func(t *testing.T) {
		rows := newRows(nil, [][]any{{"a"}})
		rows.Next()
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if got != "a" {
			t.Fatalf("Scan() got %#v, want %q", got, "a")
		}
	})

	t.Run("nil", func(t *testing.T) {
		rows := newRows(nil, [][]any{{nil}})
		rows.Next()
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if got != nil {
			t.Fatalf("Scan() got %#v, want nil", got)
		}
	})

	t.Run("bool", func(t *testing.T) {
		rows := newRows(nil, [][]any{{true}})
		rows.Next()
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if got != true {
			t.Fatalf("Scan() got %#v, want true", got)
		}
	})

	t.Run("real", func(t *testing.T) {
		rows := newRows(nil, [][]any{{3.14}})
		rows.Next()
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if got != 3.14 {
			t.Fatalf("Scan() got %#v, want 3.14", got)
		}
	})
}

func TestRowsScanUnsupportedDestinations(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})
	rows.Next()

	if err := rows.Scan(nil); !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan(nil) = %v, want ErrUnsupportedScanType", err)
	}

	var i int
	if err := rows.Scan(i); !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan(non-pointer) = %v, want ErrUnsupportedScanType", err)
	}
}

func TestRowsScanBoolSupport(t *testing.T) {
	rows := newRows(nil, [][]any{{true}})
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var b bool
	if err := rows.Scan(&b); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if !b {
		t.Fatalf("Scan() got %v, want true", b)
	}
}

func TestRowsScanRealSupport(t *testing.T) {
	rows := newRows(nil, [][]any{{3.14}})
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var f float64
	if err := rows.Scan(&f); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if f != 3.14 {
		t.Fatalf("Scan() got %v, want 3.14", f)
	}
}

func TestRowsScanDeferredQueryErrorPassthrough(t *testing.T) {
	wantErr := errors.New("deferred")
	rows := &Rows{idx: -1, err: wantErr}

	var i int
	if err := rows.Scan(&i); !errors.Is(err, wantErr) {
		t.Fatalf("Scan() = %v, want %v", err, wantErr)
	}
}

func TestRowsScanClosedRows(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})
	if err := rows.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var i int
	if err := rows.Scan(&i); !errors.Is(err, ErrRowsClosed) {
		t.Fatalf("Scan() on closed rows = %v, want ErrRowsClosed", err)
	}
}
