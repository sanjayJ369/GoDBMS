package db

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// node types
const (
	// types
	QL_UINT = 0
	QL_STR  = TYPE_BYTES
	QL_I64  = TYPE_INT64
	QL_SYM  = 3

	// binary operators
	QL_CMP_GE = 10 // >=
	QL_CMP_GT = 11 // >
	QL_CMP_LE = 12 // <=
	QL_CMP_LT = 13 // <
	QL_OR     = 14 // or
	QL_AND    = 15 // and
	QL_ADD    = 16 // +
	QL_SUB    = 17 // -
	QL_MOD    = 18 // %
	QL_MUL    = 19 // *
	QL_DIV    = 20 // /
	QL_BOOL   = 21
	QL_CMP_EQ = 22 // ==

	// unary operators
	QL_NEG = 30 // -

	// types
	QL_TUP = 40 // tuple
)

var KeyWords = []string{"create", "table", "select", "index", "by",
	"insert", "into", "delete", "from", "update", ",",
	"(", ")", "-", "as", "or", "and", "+", "*", "/", "%",
	">=", "<=", "==", ">", "<"}

// isValidKeyword reports if input is a valid symbol(keyword)
func isValidKeyword(s string) bool {
	for _, tkn := range KeyWords {
		if s == tkn {
			return true
		}
	}
	return false
}

type QLNode struct {
	Value
	Kids []QLNode
}

type Parser struct {
	input []byte
	idx   int
	err   error
}

///////////////// expression tree evaluvation /////////////////

type QLEvalContext struct {
	env Record
	out Value
	err error
}

func qlEval(ctx *QLEvalContext, node QLNode) {
	switch node.Type {
	case QL_STR, QL_I64:
		ctx.out = node.Value
	case QL_SYM:
		if v := ctx.env.Get(string(node.Str)); v != nil {
			ctx.out = *v
		} else {
			qlErr(ctx, "unknown column: %s", node.Str)
		}
	case QL_CMP_GE, QL_CMP_LE, QL_CMP_LT, QL_CMP_GT, QL_CMP_EQ:
		qlCmp(ctx, &node, node.Type)
	case QL_OR, QL_AND:
		qlOrAnd(ctx, &node, node.Type)
	case QL_ADD, QL_SUB, QL_MUL, QL_DIV, QL_MOD:
		qlNumeric(ctx, &node, node.Type)
	case QL_NEG:
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == QL_I64 {
			ctx.out.I64 = -ctx.out.I64
		} else {
			qlErr(ctx, "QL_NEG type error")
		}
	}
}

func qlNumeric(ctx *QLEvalContext, node *QLNode, ntype uint32) {
	left, right, err := evalLeftRightKids(ctx, node)
	if right == nil {
		right = &Value{}
	}
	if err != nil {
		return
	}

	if left.Type != QL_I64 {
		qlErr(ctx, "invalid kid nodes: %v and %v", left, right)
		return
	}

	out := Value{Type: QL_I64}
	var res int64
	a := left.I64
	b := right.I64
	switch ntype {
	case QL_ADD:
		res = a + b
	case QL_SUB:
		res = a - b
	case QL_MUL:
		res = a * b
	case QL_DIV:
		res = a / b
	case QL_MOD:
		res = a % b
	case QL_NEG:
		res = -a
	default:
		qlErr(ctx, "invalid node type: %d", ntype)
		return
	}
	out.I64 = res
	ctx.out = out
}

// qlOrAnd is a generatic function to evalue
// OR and AND binary operators
func qlOrAnd(ctx *QLEvalContext, node *QLNode, cmp uint32) {
	left, right, err := evalLeftRightKids(ctx, node)
	if err != nil {
		return
	}

	if left.Type != QL_BOOL {
		qlErr(ctx, "invalid kid nodes: %v and %v", left, right)
		return
	}

	res := Value{Type: QL_BOOL}
	res.I64 = 0

	a := false
	b := false

	if right.I64 == 1 {
		b = true
	}
	if left.I64 == 1 {
		a = true
	}

	switch cmp {
	case QL_OR:
		if a || b {
			res.I64 = 1
		}
	case QL_AND:
		if a && b {
			res.I64 = 1
		}
	}

	ctx.out = res
}

// qlCmp is a generic function to evaluvate the comparition nodes
// type QL_BOOL (I64)
// 0 -> false
// 1 -> true
func qlCmp(ctx *QLEvalContext, node *QLNode, cmp uint32) {
	left, right, err := evalLeftRightKids(ctx, node)
	if err != nil {
		return
	}

	if left.Type != QL_I64 && left.Type != QL_STR {
		qlErr(ctx, "invalid kid nodes: %v and %v", left, right)
		return
	}

	res := Value{Type: QL_BOOL}
	res.I64 = 0
	if left.Type == QL_I64 {
		a := left.I64
		b := right.I64
		switch cmp {
		case QL_CMP_EQ:
			if a == b {
				res.I64 = 1
			}
		case QL_CMP_GE:
			if a == b || a > b {
				res.I64 = 1
			}
		case QL_CMP_LE:
			if a == b || a < b {
				res.I64 = 1
			}
		case QL_CMP_GT:
			if a > b {
				res.I64 = 1
			}
		case QL_CMP_LT:
			if a < b {
				res.I64 = 1
			}
		}
		ctx.out = res
	} else if left.Type == QL_STR {
		a := left.Str
		b := right.Str
		c := strings.Compare(string(a), string(b))
		switch cmp {
		case QL_CMP_EQ:
			if c == 0 {
				res.I64 = 1
			}
		case QL_CMP_GE:
			if c == 0 || c == 1 {
				res.I64 = 1
			}
		case QL_CMP_LE:
			if c == 0 || c == -1 {
				res.I64 = 1
			}
		case QL_CMP_GT:
			if c == 1 {
				res.I64 = 1
			}
		case QL_CMP_LT:
			if c == -1 {
				res.I64 = 1
			}
		}
		ctx.out = res
	} else {
		qlErr(ctx, "invalid type: %d, expected string, number", left.Type)
	}
}

func evalLeftRightKids(ctx *QLEvalContext, node *QLNode) (*Value, *Value, error) {
	ctxLeft := &QLEvalContext{env: ctx.env}
	qlEval(ctxLeft, node.Kids[0])
	if ctxLeft.err != nil {
		qlErr(ctx, "evaluvating node: %v: err: %w", node.Kids[0], ctxLeft.err)
		return nil, nil, fmt.Errorf("evaluvating node: %v: err: %w", node.Kids[0], ctxLeft.err)
	}

	// nodes like neg have only one kid node
	if len(node.Kids) == 1 {
		return &ctxLeft.out, nil, nil
	}

	ctxRight := &QLEvalContext{env: ctx.env}
	qlEval(ctxRight, node.Kids[1])
	if ctxRight.err != nil {
		qlErr(ctx, "evaluvating node: %v: err: %w", node.Kids[1], ctxLeft.err)
		return nil, nil, fmt.Errorf("evaluvating node: %v: err: %w", node.Kids[1], ctxLeft.err)
	}

	if ctxLeft.out.Type != ctxRight.out.Type {
		qlErr(ctx, "comparing different types: %d and %d", ctxLeft.out.Type, ctxRight.out.Type)
		return nil, nil, fmt.Errorf("comparing different types: %d and %d", ctxLeft.out.Type, ctxRight.out.Type)
	}

	return &ctxLeft.out, &ctxRight.out, nil
}

func qlErr(ctx *QLEvalContext, args ...interface{}) {
	err := ctx.err
	if err != nil {
		format := err.Error() + " : " + args[0].(string)
		ctx.err = fmt.Errorf(format, args[1:]...)
	} else {
		ctx.err = fmt.Errorf(args[0].(string), args[1:]...)
	}
}

///////////////// expression tree parsing /////////////////

func (p *Parser) skipSpace() {
	for p.idx < len(p.input) && p.input[p.idx] == byte(32) {
		p.idx++
	}
}

// pkeyword reports if the key words are valid
func pkeyword(p *Parser, kwds ...string) bool {
	save := p.idx
	for _, kw := range kwds {
		p.skipSpace()
		start := p.idx
		end := start + len(kw)

		if end > len(p.input) {
			p.idx = save
			return false
		}

		ok := strings.EqualFold(string(p.input[start:end]), kw)
		sym := isValidKeyword(kw)

		// if it is not a valid keyword or keyword does not match
		if !ok || !sym {
			p.idx = save
			return false
		}
		p.idx = end
	}
	return true
}

// parsing expressions
// operator presidence
// unary operator > mul, div, mod > add, sub > comparition > NOT > AND > OR

// parsing tuple
func pExprTuple(p *Parser, node *QLNode) {
	kids := []QLNode{{}}
	pExprOr(p, &kids[0])
	for pkeyword(p, ",") {
		kids = append(kids, QLNode{})
		pExprOr(p, &kids[len(kids)-1])
	}

	if len(kids) > 1 {
		node.Type = QL_TUP // it is a tuple
		node.Kids = kids
	} else {
		*node = kids[0] // not a tuple
	}
}

// generalized binary operator parser
func pExprBinOp(
	p *Parser, node *QLNode,
	ops []string, types []uint32, next func(*Parser, *QLNode),
) {
	if len(ops) != len(types) {
		panic("invalid length of operators and types in parsing binary operator")
	}
	// reccursively parse the left part of the expression
	// according to the order of precidence
	left := QLNode{}
	next(p, &left)

	for more := true; more; {
		more = false
		for i := range ops {
			if pkeyword(p, ops[i]) {
				new := QLNode{Value: Value{Type: types[i]}}
				new.Kids = []QLNode{left, {}}
				next(p, &new.Kids[1])

				left = new
				more = true
				break
			}
		}
	}

	*node = left
}

// binary operator parsers
// or
func pExprOr(p *Parser, node *QLNode) {
	pExprBinOp(p, node, []string{"or"}, []uint32{QL_OR}, pExprAnd)
}

// and
func pExprAnd(p *Parser, node *QLNode) {
	pExprBinOp(p, node, []string{"and"}, []uint32{QL_AND}, pExprCmp)
}

// cmp
func pExprCmp(p *Parser, node *QLNode) {
	pExprBinOp(p, node, []string{"<=", ">=", "==", "<", ">"}, []uint32{QL_CMP_LE, QL_CMP_GE, QL_CMP_EQ, QL_CMP_LT, QL_CMP_GT}, pExprAdd)
}

// add, sub
func pExprAdd(p *Parser, node *QLNode) {
	pExprBinOp(p, node, []string{"+", "-"}, []uint32{QL_ADD, QL_SUB}, pExprMul)
}

// mul, div. mod
func pExprMul(p *Parser, node *QLNode) {
	pExprBinOp(p, node, []string{"*", "/", "%"}, []uint32{QL_MUL, QL_DIV, QL_MOD}, pExprUnOp)
}

// unary operator parsers
func pExprUnOp(p *Parser, node *QLNode) {
	switch {
	case pkeyword(p, "-"):
		node.Type = QL_NEG
		node.Kids = []QLNode{{}}
		pExprAtom(p, &node.Kids[0])
	default:
		pExprAtom(p, node)
	}
}

func pExprAtom(p *Parser, node *QLNode) {
	switch {
	case pkeyword(p, "("):
		pExprTuple(p, node)
		if !pkeyword(p, ")") {
			pErr(p, node, "unclosed paranthesis")
		}
	case pSym(p, node):
	case pNum(p, node):
	case pStr(p, node):
	default:
		pErr(p, node, "expected symbol, number or string")
	}
}

func isSym(ch byte) bool {
	r := rune(ch)
	return unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_'
}

func isSymStart(ch byte) bool {
	r := rune(ch)
	return r == '@'
}

func isStr(ch byte) bool {
	r := rune(ch)
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}

func isNum(ch byte) bool {
	r := rune(ch)
	return unicode.IsNumber(r)
}

func pNum(p *Parser, node *QLNode) bool {
	p.skipSpace()

	ptr := p.idx
	if ptr >= len(p.input) {
		return false
	}
	for ptr < len(p.input) && isNum(p.input[ptr]) {
		ptr++
	}

	node.Type = QL_I64
	num, err := strconv.Atoi(string(p.input[p.idx:ptr]))
	if err != nil {
		return false
	}
	node.I64 = int64(num)
	p.idx = ptr
	return true
}

func pStr(p *Parser, node *QLNode) bool {
	p.skipSpace()

	ptr := p.idx
	if ptr >= len(p.input) {
		return false
	}

	for ptr < len(p.input) && isStr(p.input[ptr]) {
		ptr++
	}
	if isValidKeyword(string(p.input[p.idx:ptr])) {
		return false // key words are not allowed
	}
	node.Type = QL_STR
	node.Str = p.input[p.idx:ptr]
	p.idx = ptr
	return true
}

// pSym parsers symbols
// @ character is used to represent start of a symbol
func pSym(p *Parser, node *QLNode) bool {
	p.skipSpace()

	ptr := p.idx
	if ptr >= len(p.input) || !isSymStart(p.input[ptr]) {
		return false
	}
	p.idx++ // skip the '@'character
	ptr++
	for ptr < len(p.input) && isSym(p.input[ptr]) {
		ptr++
	}
	if isValidKeyword(string(p.input[p.idx:ptr])) {
		return false // key words are not allowed
	}

	node.Type = QL_SYM
	node.Str = p.input[p.idx:ptr]
	p.idx = ptr
	return true
}

func pErr(p *Parser, node *QLNode, msg string) {
	if p.err == nil {
		p.err = fmt.Errorf("%s", msg)
	} else {
		p.err = fmt.Errorf("%w: %s", p.err, msg)
	}
}

///////////////// statements parsing /////////////////

// possible statements:
// create table ...  -> to create a new table
// select ...		 -> to select records
// insert into ...	 -> insert records into table
// update ... 		 -> update table values
// delete from ...   -> delete records from table

// scanner to scan thorugh range of values
type QLScan struct {
	Table  string // table name
	Key1   *QLNode
	Key2   *QLNode
	Filter QLNode
	Offset int
	Limit  int
}

// select tuple from table
type QLSelect struct {
	QLScan
	Name   []string
	Output []QLNode
}

// update tuple values
type QLUpdate struct {
	QLScan
	Name   []string
	Values []QLNode
}

// insert tuples into record
type QLInsert struct {
	Table  string
	Name   []string
	Values [][]QLNode
}

// delete tuples from table
type QLDelete struct {
	QLScan
}

// create a new table
type QLCreateTable struct {
	Def TableDef
}

// pStmt parsers the statement
func pStmt(p *Parser) (r interface{}) {
	switch {
	// case pkeyword(p, "create", "table"):
	// 	r = pCreateTable(p)
	case pkeyword(p, "select"):
		r = pSelect(p)
	// case pkeyword(p, "insert", "into"):
	// 	r = pInsert(p)
	// case pkeyword(p, "update"):
	// 	r = pUpdate(p)
	// case pkeyword(p, "delete", "from"):
	// 	r = pDelete(p)
	default:
		panic(fmt.Sprintf("invalid query statement: %s", string(p.input)))
	}
	return r
}

func pMustSym(p *Parser) string {
	p.skipSpace()
	ptr := p.idx
	if ptr >= len(p.input) {
		return ""
	}

	for ptr < len(p.input) && isSym(p.input[ptr]) {
		ptr++
	}

	sym := string(p.input[p.idx:ptr])
	p.idx = ptr
	return sym
}

// condition1 AND condition2 AND condtion...
func pIndexBy(p *Parser, node *QLScan) {
	conditions := []QLNode{{}}
	pExprCmp(p, &conditions[0])

	// parse conditiosn
	for pkeyword(p, "and") {
		new := QLNode{}
		pExprCmp(p, &new)
		conditions = append(conditions, new)
	}

	if len(conditions) > 2 {
		pErr(p, nil, "more then two conditions")
	}

	if len(conditions) == 1 {
		node.Key1 = &conditions[0]
		node.Key2 = nil
		return
	}

	node.Key1 = &conditions[0]
	node.Key2 = &conditions[1]
}

// INDEX BY ... FILTER ... LIMIT ...
func pScan(p *Parser, node *QLScan) {
	if pkeyword(p, "index", "by") {
		pIndexBy(p, node)
	}
	if pkeyword(p, "filter") {
		pExprOr(p, &node.Filter)
	}
	node.Offset, node.Limit = 0, math.MaxInt64
	if pkeyword(p, "limit") {
		pLimit(p, node)
	}
}

func pLimit(p *Parser, node *QLScan) {
	p.skipSpace()
	res := QLNode{}
	ok := pNum(p, &res)
	if !ok {
		pErr(p, &res, "expected number after limit")
	}
	node.Limit = int(res.I64)
}

func pSelect(p *Parser) *QLSelect {
	stmt := QLSelect{}
	// select statement structure
	// SELECT ... FROM table_name INDEX BY ... FILTER ... LIMIT ...

	// SELECT ...
	pSelectExprList(p, &stmt)

	// FROM table_name
	if !pkeyword(p, "from") {
		pErr(p, nil, "exprected `from` table")
	}
	stmt.Table = pMustSym(p)

	// INDEX BY ... FILTER ... LIMIT ...
	pScan(p, &stmt.QLScan)
	if p.err != nil {
		return nil
	}
	return &stmt
}

func pSelectExprList(p *Parser, stmt *QLSelect) {
	pSelectExpr(p, stmt)
	for pkeyword(p, ",") {
		pSelectExpr(p, stmt)
	}
}

func pSelectExpr(p *Parser, node *QLSelect) {
	expr := QLNode{}
	pExprOr(p, &expr)
	name := string(expr.Str)
	if pkeyword(p, "as") {
		name = pMustSym(p)
	}

	node.Name = append(node.Name, name)
	node.Output = append(node.Output, expr)
}

// /////////////// statements execution /////////////////
func qlScanInit(req *QLScan, tx *DBTX) (*Scanner, error) {
	tname := req.Table
	tdef, err := getTableDef(tx, tname)
	if err != nil {
		return nil, fmt.Errorf("no table named: %s", tdef.Name)
	}

	key1node := req.Key1
	key1col := key1node.Kids[0].Str

	var valType uint32
	for i, col := range tdef.Cols {
		if string(key1col) == col {
			valType = tdef.Types[i]
		}
	}

	// determine to which index does the query should refer
	var index int
	for i, idx := range tdef.Indexes {
		if string(key1col) == idx[0] {
			index = i
		}
	}

	if req.Key2 == nil {
		startRec := &Record{}
		err := addValToRecord(valType, startRec, string(key1col), key1node.Kids[1].Value)
		if err != nil {
			return nil, fmt.Errorf("adding start record: %w", err)
		}

		endRec := &Record{}
		maxValue := Value{
			I64: math.MaxInt64,
			Str: []byte{0xff, 0xff, 0xff, 0xff},
		}
		minValue := Value{
			I64: math.MinInt64 + 1,
			Str: []byte{0x00, 0x00, 0x00, 0x00},
		}

		if key1node.Type == QL_CMP_GE || key1node.Type == QL_CMP_GT {
			err := addValToRecord(valType, endRec, string(key1col), maxValue)
			if err != nil {
				return nil, fmt.Errorf("adding start record: %w", err)
			}
		} else if key1node.Type == QL_CMP_LE || key1node.Type == QL_CMP_LT {
			err := addValToRecord(valType, endRec, string(key1col), minValue)
			if err != nil {
				return nil, fmt.Errorf("adding start record: %w", err)
			}
		} else if key1node.Type == QL_CMP_EQ {
			endRec = startRec
		}

		key1inc := false
		key2inc := false
		if key1node.Type == QL_CMP_GE || key1node.Type == QL_CMP_LE || key1node.Type == QL_CMP_EQ {
			key1inc = true
		}
		if key1node.Type == QL_CMP_EQ {
			key2inc = true
		}
		sc := tx.NewScanner(*tdef, *startRec, *endRec, key1inc, key2inc, index)
		return sc, nil
	}

	// both the conditions are given
	key2node := req.Key2
	key2col := key2node.Kids[0].Str

	if key1node.Type == QL_CMP_GE || key1node.Type == QL_CMP_GT {
		if key2node.Type != QL_CMP_LE && key2node.Type != QL_CMP_LT {
			return nil, fmt.Errorf("invalid range conitions")
		}
	}

	if key2node.Type == QL_CMP_GE || key2node.Type == QL_CMP_GT {
		if key1node.Type != QL_CMP_LE && key1node.Type != QL_CMP_LT {
			return nil, fmt.Errorf("invalid range conitions")
		}
	}

	if string(key1col) != string(key2col) {
		return nil, fmt.Errorf("index by should include conditions for the same column")
	}

	// create records
	startRec := &Record{}
	endRec := &Record{}

	if valType == QL_STR {
		startRec.AddStr(string(key1col), key1node.Kids[1].Str)
		endRec.AddStr(string(key2col), key2node.Kids[1].Str)
	} else if valType == QL_I64 {
		startRec.AddI64(string(key1col), key1node.Kids[1].I64)
		endRec.AddI64(string(key2col), key2node.Kids[1].I64)
	} else {
		return nil, fmt.Errorf("invalid column type: %d", valType)
	}

	// determine whether to include the edge keys or not
	var key1inc bool
	var key2inc bool
	if key1node.Type == QL_CMP_GE || key1node.Type == QL_CMP_LE {
		key1inc = true
	}
	if key2node.Type == QL_CMP_GE || key2node.Type == QL_CMP_LE {
		key2inc = true
	}

	sc := tx.NewScanner(*tdef, *startRec, *endRec, key1inc, key2inc, index)
	return sc, nil
}

func addValToRecord(valType uint32, rec *Record, col string, val Value) error {
	if valType == QL_STR {
		rec.AddStr(string(col), val.Str)
	} else if valType == QL_I64 {
		rec.AddI64(string(col), val.I64)
	} else {
		return fmt.Errorf("invalid column type: %d", valType)
	}
	return nil
}
