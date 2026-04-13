package parser

const (
	untypedIntegerLiteralMin = -2147483648
	untypedIntegerLiteralMax = 2147483647
)

func untypedIntegerLiteralInRange(v int64) bool {
	return v >= untypedIntegerLiteralMin && v <= untypedIntegerLiteralMax
}

// parseUntypedIntegerLiteral parses the intentionally narrow legacy SQL integer
// literal range that still remains untyped until a later exact-width context.
func parseUntypedIntegerLiteral(token string) (int64, bool) {
	value, err := parseInt64Literal(token)
	if err != nil {
		return 0, false
	}
	if !untypedIntegerLiteralInRange(value) {
		return 0, false
	}
	return value, true
}
