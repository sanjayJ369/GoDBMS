package db

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Expression struct {
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
		exprs := []Expression{
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

		binOperTypes := map[string]int{"or": QL_OR, "and": QL_AND, "+": QL_AND, "-": QL_SUB,
			"*": QL_MUL, "/": QL_DIV, "%": QL_MOD, ">=": QL_CMP_GE, ">": QL_CMP_GT,
			"<=": QL_CMP_LE, "<": QL_CMP_LT}

		for kw := range binOperTypes {
			operType := binOperTypes[kw]
			for _, expr := range exprs {
				newExpr := strings.Replace(expr.expr, "or", kw, 1)
				if kw == "*" || kw == "+" {
					fmt.Println("breakpoint")
				}
				p := Parser{
					input: []byte(newExpr),
				}
				got := QLNode{}
				pExprTuple(&p, &got)
				fmt.Println(newExpr)
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

func assertNodeValue(t testing.TB, node QLNode, val Value) {
	switch node.Type {
	case QL_STR | QL_SYM:
		assert.Equal(t, string(val.Str), string(node.Str))
	case QL_I64:
		assert.Equal(t, int(val.I64), int(node.I64))
	}
}
