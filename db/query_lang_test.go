package db

import (
	"dbms/kv"
	"dbms/util"
	"fmt"
	"log"
	"math/rand/v2"
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
			input: "2 == 2",
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
			fmt.Println(expr.input)
			if expr.input == "2 == 2" {
				fmt.Println("breakpoint")
			}
			pExprTuple(&p, &node)
			qlEval(&expr.ctx, node)
			got := expr.ctx.out

			assert.NoError(t, expr.ctx.err)
			assert.Equal(t, expr.I64, got.I64)
		}
	})
}

func TestPSelect(t *testing.T) {

	t.Run("parsing select statement with 2 index by conditions", func(t *testing.T) {
		expr := "select a as a1, b as a2 from demo index by a > 10 and a <= 30"
		p := &Parser{
			input: []byte(expr),
		}
		pkeyword(p, "select")
		res := pSelect(p)

		assert.NoError(t, p.err)
		// assert table name
		assert.Equal(t, "demo", res.Table)

		// assert column names
		assert.Equal(t, "a1", res.Name[0])
		assert.Equal(t, "a2", res.Name[1])

		// assert scanner nodes
		key1node := res.Key1
		key2node := res.Key2
		assert.Equal(t, QL_CMP_GT, int(key1node.Type))
		assert.Equal(t, QL_CMP_LE, int(key2node.Type))
	})

	t.Run("parsing select statement with 1 index by conditions", func(t *testing.T) {
		expr := "select a as a1, b as a2 from demo index by a == 10"
		p := &Parser{
			input: []byte(expr),
		}
		pkeyword(p, "select")
		res := pSelect(p)

		assert.NoError(t, p.err)
		// assert table name
		assert.Equal(t, "demo", res.Table)

		// assert column names
		assert.Equal(t, "a1", res.Name[0])
		assert.Equal(t, "a2", res.Name[1])

		// assert scanner nodes
		key1node := res.Key1
		assert.Equal(t, QL_CMP_EQ, int(key1node.Type))
	})

	t.Run("parsing select statement with no index by conditions(full table scan)", func(t *testing.T) {
		expr := "select a as a1, b as a2 from demo"
		p := &Parser{
			input: []byte(expr),
		}
		pkeyword(p, "select")
		res := pSelect(p)

		assert.NoError(t, p.err)
		// assert table name
		assert.Equal(t, "demo", res.Table)

		// assert column names
		assert.Equal(t, "a1", res.Name[0])
		assert.Equal(t, "a2", res.Name[1])

		// assert scanner nodes
		assert.Nil(t, res.Key1)
		assert.Nil(t, res.Key2)
	})
}

func TestPScanFuncs(t *testing.T) {
	t.Run("pIndexBy parser equallity condition", func(t *testing.T) {
		expr := "a == 10"
		p := &Parser{
			input: []byte(expr),
		}
		sc := &QLScan{}

		pIndexBy(p, sc)
		kid1 := sc.Key1
		lchild := kid1.Kids[0]
		rchild := kid1.Kids[1]

		assert.Equal(t, QL_CMP_EQ, int(kid1.Type))
		assert.Equal(t, "a", string(lchild.Str))
		assert.Equal(t, 10, int(rchild.I64))
		assert.Nil(t, sc.Key2)
	})

	t.Run("pIndexBy parser comparition condition", func(t *testing.T) {
		expr := "a > 10 and a < 20"
		p := &Parser{
			input: []byte(expr),
		}
		sc := &QLScan{}

		pIndexBy(p, sc)
		// checking first condition
		kid := sc.Key1
		lchild := kid.Kids[0]
		rchild := kid.Kids[1]

		assert.Equal(t, QL_CMP_GT, int(kid.Type))
		assert.Equal(t, "a", string(lchild.Str))
		assert.Equal(t, 10, int(rchild.I64))

		// checking second condition
		kid = sc.Key2
		lchild = kid.Kids[0]
		rchild = kid.Kids[1]

		assert.Equal(t, QL_CMP_LT, int(kid.Type))
		assert.Equal(t, "a", string(lchild.Str))
		assert.Equal(t, 20, int(rchild.I64))

	})
}

func TestQLScan(t *testing.T) {

	loc := util.NewTempFileLoc()
	kvstore, err := kv.NewKv(loc)
	if err != nil {
		log.Fatalf("creating kvstore: %s", err.Error())
	}
	defer kvstore.Close()

	var tdef = &TableDef{
		Name:    "demo",
		Cols:    []string{"id", "a", "b", "c", "data"},
		Types:   []uint32{TYPE_INT64, TYPE_INT64, TYPE_INT64, TYPE_INT64, TYPE_BYTES},
		Pkeys:   1,
		Indexes: [][]string{{"id"}, {"a"}, {"b"}, {"c"}, {"a", "b"}, {"a", "b", "c"}},
	}

	database := NewDB(loc, kvstore)

	_ = insertRecords(t, tdef, database)

	t.Run("evaluvating select query", func(t *testing.T) {
		tx := database.NewTX()
		database.Begin(tx)

		query := "select a, b from demo index by @a > 5 and @a < 8"
		p := &Parser{
			input: []byte(query),
		}
		pkeyword(p, "select")
		res := pSelect(p)
		assert.NoError(t, p.err)
		sc, err := qlScanInit(&res.QLScan, tx)
		assert.NoError(t, err)
		for sc.Valid() {
			rec, err := sc.Deref()
			if err != nil {
				log.Fatalf("derefercing row: %s", err.Error())
			}
			sc.Next()
			assert.Less(t, rec.Get("a").I64, int64(8))
			assert.Greater(t, rec.Get("a").I64, int64(5))
		}
	})

	t.Run("evaluvating select query with single condition (equality)", func(t *testing.T) {
		tx := database.NewTX()
		database.Begin(tx)

		query := "select a, b from demo index by @a == 6"
		p := &Parser{
			input: []byte(query),
		}
		pkeyword(p, "select")
		res := pSelect(p)
		assert.NoError(t, p.err)
		sc, err := qlScanInit(&res.QLScan, tx)
		assert.NoError(t, err)
		for sc.Valid() {
			rec, err := sc.Deref()
			if err != nil {
				log.Fatalf("derefercing row: %s", err.Error())
			}
			sc.Next()
			assert.Equal(t, rec.Get("a").I64, int64(6))
		}
	})

	t.Run("evaluvating select query with single condition (comparition)", func(t *testing.T) {
		tx := database.NewTX()
		database.Begin(tx)

		query := "select a, b from demo index by @a >= 6"
		p := &Parser{
			input: []byte(query),
		}
		pkeyword(p, "select")
		res := pSelect(p)
		assert.NoError(t, p.err)
		sc, err := qlScanInit(&res.QLScan, tx)
		assert.NoError(t, err)
		for sc.Valid() {
			rec, err := sc.Deref()
			if err != nil {
				log.Fatalf("derefercing row: %s", err.Error())
			}
			sc.Next()
			assert.GreaterOrEqual(t, rec.Get("a").I64, int64(6))
		}
	})

	t.Run("evaluvating select query with single condition (comparition) and filter", func(t *testing.T) {
		tx := database.NewTX()
		database.Begin(tx)

		query := "select a, b from demo index by @a >= 6 filter @c > 1 and @c < 6 and @b == 3"
		p := &Parser{
			input: []byte(query),
		}
		pkeyword(p, "select")
		res := pSelect(p)
		assert.NoError(t, p.err)
		sc, err := qlScanInit(&res.QLScan, tx)
		iterator := newQlScanIter(&res.QLScan, *sc)
		assert.NoError(t, err)
		for iterator.Valid() {
			iterator.Deref()
			rec := iterator.rec
			err = iterator.err
			if err == nil {
				fmt.Println("id: ", rec.Get("id").I64, " : ", "a: ", rec.Get("a").I64,
					" : ", "b: ", rec.Get("b").I64, " : ", "c: ", rec.Get("c").I64)
				assert.Greater(t, rec.Get("c").I64, int64(1))
				assert.Less(t, rec.Get("c").I64, int64(6))
			}
			sc.Next()
		}
	})

	t.Run("evaluvating select query with single condition and  offset, limit", func(t *testing.T) {
		tx := database.NewTX()
		database.Begin(tx)

		query := "select a, b from demo index by @id > 10 offset 5 limit 10"
		p := &Parser{
			input: []byte(query),
		}
		pkeyword(p, "select")
		res := pSelect(p)
		assert.NoError(t, p.err)
		iterator, err := newQlScanFilter(&res.QLScan, tx)
		assert.NoError(t, err)
		// offset is 5
		i := 11 + 5
		for iterator.Valid() {
			rec, err := iterator.Deref()
			assert.NoError(t, err)
			fmt.Println("id: ", rec.Get("id").I64, " : ", "a: ", rec.Get("a").I64,
				" : ", "b: ", rec.Get("b").I64, " : ", "c: ", rec.Get("c").I64)
			assert.Greater(t, rec.Get("id").I64, int64(10))
			assert.Equal(t, int64(i), rec.Get("id").I64)
			i += 1
			iterator.Next()
		}
	})

	t.Run("evaluvating select query with no index by", func(t *testing.T) {
		tx := database.NewTX()
		database.Begin(tx)

		query := "select a, b, c from demo"
		p := &Parser{
			input: []byte(query),
		}
		pkeyword(p, "select")
		res := pSelect(p)
		assert.NoError(t, p.err)
		iterator, err := newQlScanFilter(&res.QLScan, tx)
		assert.NoError(t, err)

		count := 0
		for iterator.Valid() {
			rec, err := iterator.Deref()
			assert.NoError(t, err)
			iterator.Next()
			assert.Equal(t, rec.Get("id").I64, int64(count))
			count++
		}
	})

	t.Run("evaluvating select query with specifying only one index by column", func(t *testing.T) {
		tx := database.NewTX()
		database.Begin(tx)

		query := "select a, b, c from demo index by @a"
		p := &Parser{
			input: []byte(query),
		}
		pkeyword(p, "select")
		res := pSelect(p)
		assert.NoError(t, p.err)
		iterator, err := newQlScanFilter(&res.QLScan, tx)
		assert.NoError(t, err)
		oldVal := int64(0)
		for iterator.Valid() {
			rec, err := iterator.Deref()
			assert.NoError(t, err)
			newVal := rec.Get("a").I64
			assert.GreaterOrEqual(t, newVal, oldVal)
			if newVal > oldVal {
				oldVal = newVal
			}
			iterator.Next()
		}
	})

	t.Run("evaluvating select query with specifying two index by columns", func(t *testing.T) {
		tx := database.NewTX()
		database.Begin(tx)

		query := "select a, b, c from demo index by (@a, @b, @c) filter @a > 5 and @b >= 5 and @c == 4"
		p := &Parser{
			input: []byte(query),
		}
		pkeyword(p, "select")
		res := pSelect(p)
		assert.NoError(t, p.err)
		iterator, err := newQlScanFilter(&res.QLScan, tx)
		assert.NoError(t, err)
		for iterator.Valid() {
			rec, err := iterator.Deref()
			assert.NoError(t, err)
			assert.Greater(t, rec.Get("a").I64, int64(5))
			assert.GreaterOrEqual(t, rec.Get("b").I64, int64(5))
			assert.Equal(t, rec.Get("c").I64, int64(4))
			iterator.Next()
			// fmt.Println("id: ", rec.Get("id").I64, " : ", "a: ", rec.Get("a").I64,
			// 	" : ", "b: ", rec.Get("b").I64, " : ", "c: ", rec.Get("c").I64)
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

func insertRecords(t testing.TB, tdef *TableDef, database *DB) []Record {
	t.Helper()
	tx := database.NewTX()
	// begin transaction
	database.Begin(tx)
	err := tx.TableNew(tdef)
	if err != nil {
		log.Fatalf("creating new table: %s", err.Error())
	}

	// create records
	records := make([]Record, 0)
	for i := 0; i < 100; i++ {
		rec := &Record{}
		rec.AddI64("id", int64(i))
		rec.AddI64("a", int64(rand.IntN(11)))
		rec.AddI64("b", int64(rand.IntN(11)))
		rec.AddI64("c", int64(rand.IntN(11)))
		val := make([]byte, 200)
		// creating a value of size 200 bytes
		// this is done to make sure multiple nodes are created
		copy(val, []byte(fmt.Sprintf("some temp data: %d", i)))
		rec.AddStr("data", val)
		records = append(records, *rec)
	}

	// insert records
	for _, rec := range records {
		// fmt.Println("inserting row:", i)
		err := tx.Insert(tdef.Name, rec)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = database.Commit(tx)
	if err != nil {
		t.Fatal(err)
	}
	return records
}
