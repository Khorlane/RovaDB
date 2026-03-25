package parser

// DeleteStmt is the tiny parsed form for DELETE FROM ... [WHERE ...].
type DeleteStmt struct {
	TableName string
	Where     *WhereClause
	Predicate *PredicateExpr
}

func parseDelete(input string) (*DeleteStmt, error) {
	return parseDeleteTokens(input)
}
