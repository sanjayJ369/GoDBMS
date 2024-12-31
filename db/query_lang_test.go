package db

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type parsingExpr struct {
	expr      string
	rootNType int
	kid1      Value
	kid2      Value
}

func TestPKeyword(t *testing.T) {
	t.Run("pkeyword checks if the statement contains valid keywords", func(t *testing.T) {
		statements := []struct {
			query string
			kws   []string
			res   bool
		}{
			{
				query: "create table xyz",
				kws:   []string{"create", "table"},
				res:   true,
			},
			{
				query: "select *",
				kws:   []string{"select"},
				res:   true,
			},
			{
				query: "insert into",
				kws:   []string{"insert", "into"},
				res:   true,
			},
			{
				query: "don't insert into",
				kws:   []string{"insert"},
				res:   false,
			},
			{
				query: ",",
				kws:   []string{","},
				res:   true,
			},
		}

		for _, statement := range statements {
			p := &Parser{
				input: []byte(statement.query),
			}
			res := pkeyword(p, statement.kws...)
			assert.Equal(t, statement.res, res)
		}
	})
}

func TestUniaryOperatorParserFuncs(t *testing.T) {

	t.Run("pSym parses symbols", func(t *testing.T) {
		expr := "@hello"
		p := Parser{
			input: []byte(expr),
		}
		res := QLNode{}
		pSym(&p, &res)
		assert.Equal(t, QL_SYM, int(res.Type))
		assert.Equal(t, "hello", string(res.Str))
	})

	t.Run("pNum parses numbers", func(t *testing.T) {
		expr := "   000123"
		p := Parser{
			input: []byte(expr),
		}
		res := QLNode{}
		pNum(&p, &res)
		assert.Equal(t, QL_I64, int(res.Type))
		assert.Equal(t, int64(123), res.I64)
	})

	t.Run("pStr parses strings", func(t *testing.T) {
		expr := "aString"
		p := Parser{
			input: []byte(expr),
		}
		res := QLNode{}
		pStr(&p, &res)
		assert.Equal(t, QL_STR, int(res.Type))
		assert.Equal(t, expr, string(res.Str))
	})

	t.Run("parsing number with negation", func(t *testing.T) {
		expr := "-123"
		p := Parser{
			input: []byte(expr),
		}
		res := QLNode{}
		pExprUnOp(&p, &res)
		assert.Equal(t, QL_NEG, int(res.Type))
		assert.Equal(t, int64(123), res.Kids[0].I64)
	})
}

func TestParserFuncs(t *testing.T) {

	t.Run("pExprOr returns a node with OR operator", func(t *testing.T) {
		exprs := []parsingExpr{
			{
				expr:      "a or b",
				rootNType: QL_OR,
				kid1: Value{
					Type: QL_STR,
					Str:  []byte("a"),
				},
				kid2: Value{
					Type: QL_STR,
					Str:  []byte("b"),
				},
			},
			{
				expr:      "1 or @b",
				rootNType: QL_OR,
				kid1: Value{
					Type: QL_I64,
					I64:  1,
				},
				kid2: Value{
					Type: QL_SYM,
					Str:  []byte("b"),
				},
			},
			{
				expr:      "@a or 1",
				rootNType: QL_OR,
				kid1: Value{
					Type: QL_SYM,
					Str:  []byte("a"),
				},
				kid2: Value{
					Type: QL_I64,
					I64:  1,
				},
			},
			{
				expr:      "1 or 2",
				rootNType: QL_OR,
				kid1: Value{
					Type: QL_I64,
					I64:  1,
				},
				kid2: Value{
					Type: QL_I64,
					I64:  2,
				},
			},
		}

		binOperTypes := map[string]int{"or": QL_OR, "and": QL_AND, "+": QL_ADD, "-": QL_SUB,
			"*": QL_MUL, "/": QL_DIV, "%": QL_MOD, ">=": QL_CMP_GE, ">": QL_CMP_GT,
			"<=": QL_CMP_LE, "<": QL_CMP_LT}

		for kw := range binOperTypes {
			operType := binOperTypes[kw]
			for _, expr := range exprs {
				newExpr := strings.Replace(expr.expr, "or", kw, 1)
				p := Parser{
					input: []byte(newExpr),
				}
				got := QLNode{}
				pExprTuple(&p, &got)
				// check node type
				assert.Equal(t, operType, int(got.Type))

				// assert kid nodes types
				left := got.Kids[0]
				right := got.Kids[1]

				assert.Equal(t, int(expr.kid1.Type), int(left.Type))
				assert.Equal(t, int(expr.kid2.Type), int(right.Type))

				// assert kid values
				assertNodeValue(t, left, expr.kid1)
				assertNodeValue(t, right, expr.kid2)

			}
		}
	})
}

type evalExpr struct {
	Value
	input string
	ctx   QLEvalContext
}

func TestEvalFunctions(t *testing.T) {

	rec := &Record{}
	rec.AddI64("a", 1)
	rec.AddI64("b", 2)
	cmpExpr := []evalExpr{
		{
			input: "1 > 2",
			Value: Value{
				Type: QL_BOOL,
				I64:  0,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "1 < 2",
			Value: Value{
				Type: QL_BOOL,
				I64:  1,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "a < b",
			Value: Value{
				Type: QL_BOOL,
				I64:  1,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "1 >= 2",
			Value: Value{
				Type: QL_BOOL,
				I64:  0,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "1 <= 2",
			Value: Value{
				Type: QL_BOOL,
				I64:  1,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "a <= b",
			Value: Value{
				Type: QL_BOOL,
				I64:  1,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "2 >= 2",
			Value: Value{
				Type: QL_BOOL,
				I64:  1,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "2 <= 2",
			Value: Value{
				Type: QL_BOOL,
				I64:  1,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "a <= a",
			Value: Value{
				Type: QL_BOOL,
				I64:  1,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "@a <= @b",
			Value: Value{
				Type: QL_BOOL,
				I64:  1,
			},
			ctx: QLEvalContext{
				env: *rec,
			},
		},
	}
	t.Run("qlCmp evaluvates comparition operators", func(t *testing.T) {

		for _, expr := range cmpExpr {
			p := Parser{
				input: []byte(expr.input),
			}
			node := QLNode{}

			pExprTuple(&p, &node)
			qlCmp(&expr.ctx, &node, node.Type)
			got := expr.ctx.out

			assert.NoError(t, expr.ctx.err)
			assert.Equal(t, QL_BOOL, int(got.Type))
			assert.Equal(t, expr.I64, got.I64)
		}
	})

	andOrExpr := []evalExpr{
		{
			input: "(1 > 2) or (1 < 2)",
			Value: Value{
				Type: QL_BOOL,
				I64:  1,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "(1 > 2) and (1 < 2)",
			Value: Value{
				Type: QL_BOOL,
				I64:  0,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "((1 > 2) and (1 < 2)) or (5 > 4)",
			Value: Value{
				Type: QL_BOOL,
				I64:  1,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "((1 > 2) and (1 < 2)) or (5 < 4)",
			Value: Value{
				Type: QL_BOOL,
				I64:  0,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "((@a > @b) and (@b <= @b)) or (@a < @b)",
			Value: Value{
				Type: QL_BOOL,
				I64:  1,
			},
			ctx: QLEvalContext{
				env: *rec,
			},
		},
	}
	t.Run("qlOrAnd evaluvates OR and AND expressions", func(t *testing.T) {

		for _, expr := range andOrExpr {
			p := Parser{
				input: []byte(expr.input),
			}
			node := QLNode{}
			pExprTuple(&p, &node)
			qlOrAnd(&expr.ctx, &node, node.Type)
			got := expr.ctx.out

			assert.NoError(t, expr.ctx.err)
			assert.Equal(t, QL_BOOL, int(got.Type))
			assert.Equal(t, expr.I64, got.I64)
		}
	})

	numericExpr := []evalExpr{
		{
			input: "1 + 1",
			Value: Value{
				Type: QL_I64,
				I64:  2,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "1 - 1",
			Value: Value{
				Type: QL_I64,
				I64:  0,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "-2",
			Value: Value{
				Type: QL_I64,
				I64:  -2,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "5-2",
			Value: Value{
				Type: QL_I64,
				I64:  3,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "5 - 2 + 3 * 3 % 2",
			Value: Value{
				Type: QL_I64,
				I64:  4,
			},
			ctx: QLEvalContext{},
		},
		{
			input: "5 - (@b + 3 - @a) * 3 % @b",
			Value: Value{
				Type: QL_I64,
				I64:  5,
			},
			ctx: QLEvalContext{
				env: *rec,
			},
		},
	}
	t.Run("qlNumeric evaluvates numeric expressions", func(t *testing.T) {

		for _, expr := range numericExpr {
			p := Parser{
				input: []byte(expr.input),
			}
			node := QLNode{}
			pExprTuple(&p, &node)
			qlNumeric(&expr.ctx, &node, node.Type)
			got := expr.ctx.out

			assert.NoError(t, expr.ctx.err)
			assert.Equal(t, QL_I64, int(got.Type))
			assert.Equal(t, expr.I64, got.I64)
		}
	})

	t.Run("qlEval evaluvates all kinds of expresstions", func(t *testing.T) {
		allExpr := []evalExpr{}
		allExpr = append(allExpr, numericExpr...)
		allExpr = append(allExpr, cmpExpr...)
		allExpr = append(allExpr, andOrExpr...)
		for _, expr := range allExpr {
			p := Parser{
				input: []byte(expr.input),
			}
			node := QLNode{}
			pExprTuple(&p, &node)
			qlEval(&expr.ctx, node)
			got := expr.ctx.out

			assert.NoError(t, expr.ctx.err)
			assert.Equal(t, expr.I64, got.I64)
		}
	})
}

func assertNodeValue(t testing.TB, node QLNode, val Value) {
	switch node.Type {
	case QL_STR | QL_SYM:
		assert.Equal(t, string(val.Str), string(node.Str))
	case QL_I64:
		assert.Equal(t, int(val.I64), int(node.I64))
	}
}
