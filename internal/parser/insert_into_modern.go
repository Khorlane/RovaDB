package parser

import "strings"

type insertTokenParser struct {
	lexer lexer
}

func parseInsertTokens(input string) (*InsertStmt, error) {
	p := insertTokenParser{lexer: lexer{input: input}}
	return p.parse()
}

func (p *insertTokenParser) parse() (*InsertStmt, error) {
	if _, err := p.expect(tokenKeywordInsert); err != nil {
		return nil, errUnsupportedStatement
	}
	if _, err := p.expect(tokenKeywordInto); err != nil {
		return nil, errUnsupportedStatement
	}

	tableTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(tableTok.Lexeme) {
		return nil, newParseError("unsupported query form")
	}

	remainder := strings.TrimSpace(p.lexer.input[p.lexer.pos:])
	if remainder == "" {
		return nil, newParseError("unsupported query form")
	}

	columns := []string(nil)
	afterTable := remainder
	if strings.HasPrefix(afterTable, "(") {
		closeIdx := strings.Index(afterTable, ")")
		if closeIdx < 0 {
			return nil, newParseError("unsupported query form")
		}

		parsedColumns, ok := parseInsertColumns(afterTable[1:closeIdx])
		if !ok {
			return nil, newParseError("unsupported query form")
		}
		columns = parsedColumns
		afterTable = strings.TrimSpace(afterTable[closeIdx+1:])
	}

	if !strings.HasPrefix(strings.ToUpper(afterTable), "VALUES ") {
		return nil, newParseError("unsupported query form")
	}

	valuesPart := strings.TrimSpace(afterTable[len("VALUES"):])
	if !strings.HasPrefix(valuesPart, "(") || !strings.HasSuffix(valuesPart, ")") {
		return nil, newParseError("unsupported query form")
	}

	inner := strings.TrimSpace(valuesPart[1 : len(valuesPart)-1])
	if inner == "" {
		return nil, newParseError("unsupported query form")
	}

	rawValues := strings.Split(inner, ",")
	values := make([]Value, 0, len(rawValues))
	for _, raw := range rawValues {
		token := strings.TrimSpace(raw)
		if token == "" {
			return nil, newParseError("unsupported query form")
		}

		value, ok := parseLiteralValue(token)
		if !ok {
			return nil, newParseError("unsupported query form")
		}
		values = append(values, value)
	}
	if len(columns) > 0 && len(columns) != len(values) {
		return nil, newParseError("unsupported query form")
	}

	return &InsertStmt{TableName: tableTok.Lexeme, Columns: columns, Values: values}, nil
}

func (p *insertTokenParser) expect(kind tokenKind) (token, error) {
	tok, err := p.lexer.nextToken()
	if err != nil {
		return token{}, err
	}
	if tok.Kind != kind {
		return token{}, newParseError("unsupported query form")
	}
	return tok, nil
}
