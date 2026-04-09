package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

func parserValueFromPlan(value planner.Value) parser.Value {
	return value.ParserValue()
}
