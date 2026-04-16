package rovadb

import (
	"errors"
	"fmt"
)

const (
	secondsPerMinute    = 60
	minutesPerHour      = 60
	hoursPerDay         = 24
	secondsPerHour      = minutesPerHour * secondsPerMinute
	secondsPerDay       = hoursPerDay * secondsPerHour
)

var errInvalidTime = errors.New("rovadb: invalid TIME value")

// Time is the public SQL TIME scan value. It has no date or timezone
// component and is stored as seconds since midnight.
type Time struct {
	secondsSinceMidnight int32
}

// NewTime builds a validated SQL TIME value from clock components.
func NewTime(hour, minute, second int) (Time, error) {
	if hour < 0 || hour >= hoursPerDay || minute < 0 || minute >= minutesPerHour || second < 0 || second >= secondsPerMinute {
		return Time{}, errInvalidTime
	}
	return Time{
		secondsSinceMidnight: int32(hour*secondsPerHour + minute*secondsPerMinute + second),
	}, nil
}

func (t Time) Hour() int {
	return int(t.secondsSinceMidnight) / secondsPerHour
}

func (t Time) Minute() int {
	return (int(t.secondsSinceMidnight) % secondsPerHour) / secondsPerMinute
}

func (t Time) Second() int {
	return int(t.secondsSinceMidnight) % secondsPerMinute
}

func (t Time) String() string {
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
}
