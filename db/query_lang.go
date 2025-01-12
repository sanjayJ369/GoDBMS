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

var (
	maxValue = Value{
		I64: math.MaxInt64,
		Str: []byte{0xff, 0xff, 0xff, 0xff},
	}
	minValue = Value{
		I64: math.MinInt64 + 1,
		Str: []byte{0x00, 0x00, 0x00, 0x00},
	}
)

var KeyWords = []string{"create", "table", "select", "index", "by",
	"insert", "filter", "into", "delete", "from", "update", ",",
	"(", ")", "-", "as", "or", "and", "+", "*", "/", "%", ";",
	">=", "<=", "==", "=", ">", "<", "offset", "limit", "primary", "key",
	"values", "set"}

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
	for p.idx < len(p.input) && unicode.IsSpace(rune(p.input[p.idx])) {
		p.idx++
	}
}

func getNextToken(p *Parser) string {
	p.skipSpace()
	start := p.idx
	end := start + 1
	for end < len(p.input) && (isStr(p.input[end]) || isNum(p.input[end])) {
		end++
	}
	return string(p.input[start:end])
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
	Filter *QLNode
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
	Values []QLNode
}

// delete tuples from table
type QLDelete struct {
	QLScan
}

// create a new table
type QLCreateTable struct {
	Def TableDef
}

func qlCreateTable(req *QLCreateTable, tx *DBTX) error {
	return tx.TableNew(&req.Def)
}

// pStmt parsers the statement
func pStmt(p *Parser) (r interface{}) {
	switch {
	case pkeyword(p, "create", "table"):
		r = pCreateTable(p)
	case pkeyword(p, "select"):
		r = pSelect(p)
	case pkeyword(p, "insert", "into"):
		r = pInsert(p)
	// case pkeyword(p, "update"):
	// 	r = pUpdate(p)
	case pkeyword(p, "delete"):
		r = pDelete(p)
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
	// only indexes are specified
	// like: index (a, b, c)
	if pkeyword(p, "(") {
		pExprTuple(p, &conditions[0])
		if !pkeyword(p, ")") {
			pErr(p, nil, "tuple is not closed")
		}
		node.Key1 = &conditions[0]
		node.Key2 = nil
		return
	}

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
		node.Filter = &QLNode{}
		pExprOr(p, node.Filter)
	}
	node.Offset, node.Limit = 0, math.MaxInt64
	if pkeyword(p, "offset") {
		pOffset(p, node)
	}
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

func pOffset(p *Parser, node *QLScan) {
	p.skipSpace()
	res := QLNode{}
	ok := pNum(p, &res)
	if !ok {
		pErr(p, &res, "expected number after offset")
	}
	node.Offset = int(res.I64)
}

func pCreateTable(p *Parser) *QLCreateTable {
	tdef := &TableDef{}
	n := QLNode{}
	pStr(p, &n)
	tdef.Name = string(n.Str)
	if !pkeyword(p, "(") {
		pErr(p, nil, "invalid create table syntax")
		return nil
	}

	// parse table attributes
	cols := [][]string{}
	indexes := [][]string{}
	pks := []string{}

	nexttkn := getNextToken(p)
	for nexttkn != "index" && nexttkn != "primary" {
		nameNode := QLNode{}
		typeNode := QLNode{}

		pStr(p, &nameNode)
		pStr(p, &typeNode)

		col := []string{string(nameNode.Str), string(typeNode.Str)}
		cols = append(cols, col)
		if !pkeyword(p, ",") {
			pErr(p, nil, "invalid create table syntax")
		}
		nexttkn = getNextToken(p)
	}

	// parse indexes
	for nexttkn != "primary" {
		pkeyword(p, "index")
		idx := &QLNode{}
		if !pkeyword(p, "(") {
			pErr(p, nil, "invalid create table syntax (declaring index)")
		}
		pExprTuple(p, idx)
		if !pkeyword(p, ")") {
			pErr(p, nil, "invalid create table syntax (declaring index)")
		}
		if !pkeyword(p, ",") {
			pErr(p, nil, "invalid create table syntax (declaring index)")
		}

		i := []string{}
		if len(idx.Kids) == 0 {
			i = append(i, string(idx.Str))
		} else {
			for _, kid := range idx.Kids {
				i = append(i, string(kid.Str))
			}
		}
		indexes = append(indexes, i)
		nexttkn = getNextToken(p)
	}

	// parse primary keys
	if !pkeyword(p, "primary", "key") {
		pErr(p, nil, "invalid syntax, primary keys declaration")
	}
	idx := &QLNode{}
	if !pkeyword(p, "(") {
		pErr(p, nil, "no primary key")
	}
	pExprTuple(p, idx)
	if !pkeyword(p, ")") {
		pErr(p, nil, "invalid create table syntax")
	}
	if !pkeyword(p, ")") {
		pErr(p, nil, "invalid create table syntax")
	}
	if !pkeyword(p, ";") {
		pErr(p, nil, "invalid create table syntax")
	}
	if len(idx.Kids) == 0 {
		pks = append(pks, string(idx.Str))
	} else {
		for _, kid := range idx.Kids {
			pks = append(pks, string(kid.Str))
		}
	}

	// construct tdef from cols, indexes, and pks
	tdef.Pkeys = len(pks)

	for _, col := range cols {
		name := col[0]
		var colType uint32
		switch col[1] {
		case "int":
			colType = TYPE_INT64
		case "bytes":
			colType = TYPE_BYTES
		default:
			pErr(p, nil, "invalid type")
		}

		tdef.Cols = append(tdef.Cols, name)
		tdef.Types = append(tdef.Types, colType)
	}

	tdef.Indexes = append(tdef.Indexes, pks)
	tdef.Indexes = append(tdef.Indexes, indexes...)

	return &QLCreateTable{
		Def: *tdef,
	}
}

func pInsert(p *Parser) *QLInsert {
	stmt := QLInsert{}
	// get table name
	stmt.Table = pMustSym(p)
	if !pkeyword(p, "(") {
		pErr(p, nil, "invalid insert query syntax")
	}
	colNames := &QLNode{}
	pExprTuple(p, colNames)
	if !pkeyword(p, ")") {
		pErr(p, nil, "invalid insert query syntax")
	}
	if !pkeyword(p, "values") {
		pErr(p, nil, "invalid insert query syntax")
	}
	if !pkeyword(p, "(") {
		pErr(p, nil, "invalid insert query syntax")
	}
	vals := &QLNode{}
	pExprTuple(p, vals)
	if !pkeyword(p, ")") {
		pErr(p, nil, "invalid insert query syntax")
	}
	if !pkeyword(p, ";") {
		pErr(p, nil, "invalid insert query syntax")
	}

	for _, name := range colNames.Kids {
		stmt.Name = append(stmt.Name, string(name.Str))
	}
	stmt.Values = append(stmt.Values, vals.Kids...)
	return &stmt
}

func qlInsert(req *QLInsert, tx *DBTX) error {
	rec := Record{}
	// populate record with values
	for i, val := range req.Values {
		switch val.Type {
		case TYPE_BYTES:
			rec.AddStr(req.Name[i], val.Str)
		case TYPE_INT64:
			rec.AddI64(req.Name[i], val.I64)
		default:
			return fmt.Errorf("invalid type: %d", val.Type)
		}
	}
	return tx.Insert(req.Table, rec)
}

func pDelete(p *Parser) *QLDelete {
	stmt := QLDelete{}

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

func qlDelete(req *QLDelete, tx *DBTX) (int, error) {
	// scan though the records and delete
	iterator, err := newQlScanFilter(&req.QLScan, tx)
	if err != nil {
		return 0, fmt.Errorf("getting scanner: %w", err)
	}
	count := 0
	for iterator.Valid() {
		rec, err := iterator.Deref()
		if err != nil {
			return count, fmt.Errorf("getting record: %w", err)
		}
		ok := tx.Delete(req.Table, rec)
		if !ok {
			return count, fmt.Errorf("unable to delete record: %v", rec)
		}
		iterator.Next()
		count++
	}
	return count, nil
}

func pUpdate(p *Parser) *QLUpdate {
	// update tablename set a = expr, b = expr .... conditions;
	stmt := QLUpdate{}
	stmt.Table = pMustSym(p)
	if !pkeyword(p, "set") {
		pErr(p, nil, "invalid update query syntax expected 'set' ")
	}
	// a = ..., b = ..., c = ...
	pUpdateExprList(p, &stmt)

	// INDEX BY ... FILTER ... LIMIT ...
	pScan(p, &stmt.QLScan)
	if p.err != nil {
		return nil
	}
	return &stmt
}

func pUpdateExprList(p *Parser, stmt *QLUpdate) {
	pUpdateExpr(p, stmt)
	for pkeyword(p, ",") {
		pUpdateExpr(p, stmt)
	}
}

func pUpdateExpr(p *Parser, stmt *QLUpdate) {
	col := &QLNode{}
	pSym(p, col)
	stmt.Name = append(stmt.Name, string(col.Str))
	if !pkeyword(p, "=") {
		pErr(p, nil, "invalid update query syntax expected '=' ")
	}
	expr := &QLNode{}
	pExprOr(p, expr)
	stmt.Values = append(stmt.Values, *expr)
}

func qlUpdate(req *QLUpdate, tx *DBTX) error {
	iterator, err := newQlScanFilter(&req.QLScan, tx)
	if err != nil {
		return fmt.Errorf("getting scanner: %w", err)
	}

	// add colums that are not updated
	tdef, err := getTableDef(tx, req.Table)
	if err != nil {
		return fmt.Errorf("getting table defination: %w", err)
	}
	for _, col := range tdef.Cols {
		found := false
		for _, i := range req.Name {
			if i == col {
				found = true
				break
			}
		}
		if !found {
			val := QLNode{}
			val.Type = QL_SYM
			val.Str = []byte(col)
			req.Name = append(req.Name, col)
			req.Values = append(req.Values, val)
		}
	}

	for iterator.Valid() {
		rec, err := iterator.Deref()
		if err != nil {
			return err
		}
		newrec, _ := qlEvalOutput(&rec, req.Values, req.Name)
		reorderedRec := Record{}
		for i, col := range tdef.Cols {
			switch tdef.Types[i] {
			case TYPE_INT64:
				reorderedRec.AddI64(col, newrec.Get(col).I64)
			case TYPE_BYTES:
				reorderedRec.AddStr(col, newrec.Get(col).Str)
			default:
				return fmt.Errorf("invalid record type")
			}
		}
		ok := tx.Update(req.Table, reorderedRec)
		if !ok {
			return fmt.Errorf("error updating the table")
		}
		iterator.Next()
	}
	return nil
}

func pSelect(p *Parser) *QLSelect {
	stmt := QLSelect{}
	// select statement structure
	// SELECT ... FROM table_name INDEX BY ... FILTER ... LIMIT ...

	if pkeyword(p, "*") {
		// select *
		stmt.Name = append(stmt.Name, "*")
	} else {
		// SELECT ...
		pSelectExprList(p, &stmt)
	}

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
	name := ""
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

	if req.Key1 == nil && req.Key2 == nil {
		// full table scan on primary index
		// add min values to start record
		// add max values to the end record
		return fullRangeIndexScanner(tdef, tx, 0)
	}

	key1node := req.Key1

	// index by a specific column
	if len(key1node.Kids) == 0 && req.Key2 == nil {
		key1col := key1node.Str
		index, err := getColsIndex(tdef, string(key1col))
		if err != nil {
			return nil, fmt.Errorf("getting column index: %w", err)
		}
		return fullRangeIndexScanner(tdef, tx, index)
	}

	// index by multiple columns
	// ex: index by (a, b)
	if key1node.Type == QL_TUP {
		kids := key1node.Kids
		cols := []string{}
		for _, kid := range kids {
			if kid.Type != QL_SYM {
				return nil, fmt.Errorf("invalid index by arguments, expected symbols")
			}
			cols = append(cols, string(kid.Str))
		}
		index, err := getColsIndex(tdef, cols...)
		if err != nil {
			return nil, fmt.Errorf("getting column index: %w", err)
		}
		return fullRangeIndexScanner(tdef, tx, index)
	}

	key1col := key1node.Kids[0].Str

	var valType uint32
	for i, col := range tdef.Cols {
		if string(key1col) == col {
			valType = tdef.Types[i]
		}
	}

	// determine to which index does the query should refer
	index, err := getColsIndex(tdef, string(key1col))
	if err != nil {
		return nil, fmt.Errorf("getting column index: %w", err)
	}

	if req.Key2 == nil {
		startRec := &Record{}
		err := addValToRecord(valType, startRec, string(key1col), key1node.Kids[1].Value)
		if err != nil {
			return nil, fmt.Errorf("adding start record: %w", err)
		}

		endRec := &Record{}

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

type qlScanIter struct {
	req *QLScan
	sc  Scanner
	rec Record
	err error
}

func newQlScanIter(req *QLScan, sc Scanner) *qlScanIter {
	return &qlScanIter{
		req: req,
		sc:  sc,
		rec: Record{},
		err: nil,
	}
}

func (iter *qlScanIter) Valid() bool {
	return iter.sc.Valid()
}

func (iter *qlScanIter) Next() {
	iter.sc.Next()
}

func (iter *qlScanIter) Prev() {
	iter.sc.Prev()
}

func (iter *qlScanIter) Deref() (*Record, error) {
	rec, err := iter.sc.Deref()
	if err != nil {
		iter.err = err
		return nil, err
	}
	iter.rec = *rec
	var res error
	res = nil
	if iter.req.Filter != nil {
		res = qlEvalRecordBool(rec, *iter.req.Filter)
	}
	if res != nil {
		iter.err = res
		return nil, res
	}
	iter.err = nil
	return rec, nil
}

type qlScanSelect struct {
	iter *qlScanFilter
	sel  *QLSelect
}

func qlSelect(sel *QLSelect, tx *DBTX) (*qlScanSelect, error) {
	iterator, err := newQlScanFilter(&sel.QLScan, tx)
	if err != nil {
		return nil, fmt.Errorf("creating scan filter: %w", err)
	}
	return &qlScanSelect{
		iter: iterator,
		sel:  sel,
	}, nil
}

func (sc *qlScanSelect) Next() {
	sc.iter.Next()
}

func (sc *qlScanSelect) Prev() {
	sc.iter.Prev()
}

func (sc *qlScanSelect) Valid() bool {
	return sc.iter.Valid()
}

func (sc *qlScanSelect) Deref() (*Record, error) {
	rec, err := sc.iter.Deref()
	if err != nil {
		return nil, err
	}
	return qlEvalOutput(&rec, sc.sel.Output, sc.sel.Name)
}

type qlScanFilter struct {
	sc     *qlScanIter
	limit  int
	offset int
	idx    int
}

func newQlScanFilter(req *QLScan, tx *DBTX) (*qlScanFilter, error) {
	sc, err := qlScanInit(req, tx)
	if err != nil {
		return nil, err
	}
	iterator := newQlScanIter(req, *sc)
	filter := &qlScanFilter{
		sc:     iterator,
		limit:  req.Limit,
		offset: req.Offset,
		idx:    0,
	}
	for filter.idx != filter.offset {
		filter.Next()
	}
	// if starting tuple invalid
	// move right until valid tuple is found
	_, err = filter.sc.Deref()
	for err != nil && err.Error() != "out of range" {
		filter.sc.Next()
		_, err = filter.sc.Deref()
	}
	return filter, nil
}

func (iter *qlScanFilter) Valid() bool {
	if iter.idx >= iter.limit {
		return false
	}
	return iter.sc.Valid()
}

func (iter *qlScanFilter) Prev() {
	if iter.idx >= 0 {
		iter.idx--
		iter.sc.Prev()
		_, err := iter.sc.Deref()
		for err != nil {
			iter.Prev()
			_, err = iter.sc.Deref()
		}
	}
}

func (iter *qlScanFilter) Next() {
	iter.idx++
	iter.sc.Next()
	_, err := iter.sc.Deref()
	for err != nil && err.Error() != "out of range" {
		iter.sc.Next()
		_, err = iter.sc.Deref()
	}
}

func (iter *qlScanFilter) Deref() (Record, error) {
	iter.sc.Deref()
	return iter.sc.rec, iter.sc.err
}

// qlEvalOutput computes the expresstion with the rec as context
// outputs a record having the computed values,
func qlEvalOutput(rec *Record, exprs []QLNode, names []string) (*Record, error) {
	if names[0] == "*" {
		return rec, nil
	}
	out := &Record{}
	for i, expr := range exprs {
		ctx := QLEvalContext{
			env: *rec,
		}
		qlEval(&ctx, expr)
		if ctx.err != nil {
			return nil, ctx.err
		}

		switch ctx.out.Type {
		case QL_BOOL, QL_I64:
			out.AddI64(names[i], ctx.out.I64)
		case QL_STR:
			out.AddStr(names[i], ctx.out.Str)
		default:
			return nil, fmt.Errorf("invalid expression output type: %d", ctx.out.Type)
		}
	}
	return out, nil
}

func qlEvalRecordBool(rec *Record, expr QLNode) error {
	ctx := QLEvalContext{
		env: *rec,
	}
	qlEval(&ctx, expr)
	if ctx.out.Type != QL_BOOL {
		return fmt.Errorf("expression does not result in boolean outcome")
	}

	val := ctx.out
	//invalid expression
	if val.I64 == 0 {
		return fmt.Errorf("invalid")
	}
	// valid expresssion
	return nil
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

func fullRangeIndexScanner(tdef *TableDef, tx *DBTX, idx int) (*Scanner, error) {

	startRec := &Record{}
	endRec := &Record{}

	for _, col := range tdef.Indexes[idx] {
		var colType uint32
		for i, c := range tdef.Cols {
			if col == c {
				colType = tdef.Types[i]
			}
		}
		err := addValToRecord(colType, startRec, col, minValue)
		if err != nil {
			return nil, fmt.Errorf("adding start record: %w", err)
		}
		err = addValToRecord(colType, endRec, col, maxValue)
		if err != nil {
			return nil, fmt.Errorf("adding end record: %w", err)
		}
	}
	sc := tx.NewScanner(*tdef, *startRec, *endRec, false, false, idx)
	return sc, nil
}

func getColsIndex(tdef *TableDef, cols ...string) (int, error) {
	// determine to which index does the query should refer

	if len(cols) == 0 {
		return -1, fmt.Errorf("no columns provided")
	}

	if len(cols) == 1 {
		for i, idx := range tdef.Indexes {
			if cols[0] == idx[0] {
				return i, nil
			}
		}
		return -1, fmt.Errorf("there are no columsn having index: %v", cols[0])
	} else {
		// multiple cols are given (a, b, c)
		// check if there is a column in the provided order itself
		for i, idx := range tdef.Indexes {
			found := true
			if len(idx) < len(cols) {
				break
			}
			for j, col := range cols {
				if col != idx[j] {
					found = false
					break
				}
			}
			if found {
				return i, nil
			}
		}

		// atleast try to find a index which contains all the required
		// colums
		for i, idx := range tdef.Indexes {
			count := 0
			for _, col := range cols {
				for _, k := range idx {
					if col == k {
						count++
					}
				}
			}
			if count == len(cols) {
				return i, nil
			}
		}
		return -1, fmt.Errorf("there are no columsn having indexes: %v", cols)
	}
}
