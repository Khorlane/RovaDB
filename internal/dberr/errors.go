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

func NewParse(msg string) error {
	return &DBError{Kind: ErrParse, Message: msg}
}

func NewPlan(msg string) error {
	return &DBError{Kind: ErrPlan, Message: msg}
}

func NewExec(msg string) error {
	return &DBError{Kind: ErrExec, Message: msg}
}

func NewStorage(msg string) error {
	return &DBError{Kind: ErrStorage, Message: msg}
}
