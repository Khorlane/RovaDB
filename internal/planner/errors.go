package planner

import "github.com/Khorlane/RovaDB/internal/dberr"

func newPlanError(msg string) error {
	return dberr.NewPlan(msg)
}
