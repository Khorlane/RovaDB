package parser

const (
	publicIntMin = -2147483648
	publicIntMax = 2147483647
)

func publicIntInRange(v int64) bool {
	return v >= publicIntMin && v <= publicIntMax
}

func parsePublicIntLiteral(token string) (int64, bool) {
	value, err := parseInt64Literal(token)
	if err != nil {
		return 0, false
	}
	if !publicIntInRange(value) {
		return 0, false
	}
	return value, true
}
