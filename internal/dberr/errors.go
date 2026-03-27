package dberr

// ErrorKind identifies the layer that produced a structured database error.
type ErrorKind string

const (
	ErrParse   ErrorKind = "parse"
	ErrPlan    ErrorKind = "plan"
	ErrExec    ErrorKind = "execution"
	ErrStorage ErrorKind = "storage"
)

// DBError is the shared structured error shape used across internal layers.
type DBError struct {
	Kind    ErrorKind
	Message string
}

func (e *DBError) Error() string {
	return string(e.Kind) + ": " + e.Message
}

// NewParse builds a structured parse-layer error.
func NewParse(msg string) error {
	return &DBError{Kind: ErrParse, Message: msg}
}

// NewPlan builds a structured planner-layer error.
func NewPlan(msg string) error {
	return &DBError{Kind: ErrPlan, Message: msg}
}

// NewExec builds a structured execution-layer error.
func NewExec(msg string) error {
	return &DBError{Kind: ErrExec, Message: msg}
}

// NewStorage builds a structured storage-layer error.
func NewStorage(msg string) error {
	return &DBError{Kind: ErrStorage, Message: msg}
}
