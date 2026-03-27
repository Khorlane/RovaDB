package parser

// UpdateAssignment is one SET target/value pair.
type UpdateAssignment struct {
	Column string
	Value  Value
	Expr   *ValueExpr
}

// UpdateStmt is the tiny parsed form for UPDATE ... SET ... [WHERE ...].
type UpdateStmt struct {
	TableName   string
	Assignments []UpdateAssignment
	Where       *WhereClause
	Predicate   *PredicateExpr
}

func parseUpdate(input string) (*UpdateStmt, error) {
	return parseUpdateTokens(input)
}
