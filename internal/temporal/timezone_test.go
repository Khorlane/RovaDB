package temporal

import "testing"

func TestLoadLocationAcceptsRepresentativeIANAZones(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"UTC", "America/New_York", "Asia/Tokyo"} {
		location, err := LoadLocation(name)
		if err != nil {
			t.Fatalf("LoadLocation(%q) error = %v", name, err)
		}
		if location == nil {
			t.Fatalf("LoadLocation(%q) location = nil", name)
		}
		if location.String() != name {
			t.Fatalf("LoadLocation(%q) location.String() = %q, want %q", name, location.String(), name)
		}
	}
}

func TestLoadLocationRejectsInvalidOrBlankZones(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"", "   ", "Mars/Olympus", "America/Does_Not_Exist"} {
		location, err := LoadLocation(name)
		if err == nil {
			t.Fatalf("LoadLocation(%q) error = nil, want non-nil", name)
		}
		if location != nil {
			t.Fatalf("LoadLocation(%q) location = %#v, want nil", name, location)
		}
	}
}

func TestValidateLocationMatchesLoadLocationOutcome(t *testing.T) {
	t.Parallel()

	if err := ValidateLocation("Europe/Paris"); err != nil {
		t.Fatalf("ValidateLocation(valid) error = %v", err)
	}
	if err := ValidateLocation("Not/AZone"); err == nil {
		t.Fatal("ValidateLocation(invalid) error = nil, want non-nil")
	}
}
