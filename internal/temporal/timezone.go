package temporal

import (
	"fmt"
	"strings"
	"time"

	_ "time/tzdata"
)

// CurrentTimezoneBasisVersion identifies the embedded timezone-rule basis used
// by this build. Later slices can persist and validate this product-owned value.
const CurrentTimezoneBasisVersion = "embedded-tzdata-v1"

// LoadLocation resolves an IANA timezone name against the embedded tzdata set.
func LoadLocation(name string) (*time.Location, error) {
	canonicalName := strings.TrimSpace(name)
	if canonicalName == "" {
		return nil, fmt.Errorf("temporal: timezone name is required")
	}

	location, err := time.LoadLocation(canonicalName)
	if err != nil {
		return nil, fmt.Errorf("temporal: invalid timezone %q", name)
	}
	return location, nil
}

// ValidateLocation reports whether an IANA timezone name can be loaded from the
// embedded tzdata set.
func ValidateLocation(name string) error {
	_, err := LoadLocation(name)
	return err
}
