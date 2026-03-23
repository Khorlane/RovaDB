package parser

import "github.com/Khorlane/RovaDB/internal/dberr"

func newParseError(msg string) error {
	return dberr.NewParse(msg)
}
