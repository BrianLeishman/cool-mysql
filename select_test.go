package cool_test

import (
	"database/sql"
	"testing"
	"time"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	. "github.com/stretchr/testify/assert"
)

type myString struct {
	String string
}

func (z *myString) CoolMySQLScanRow(cols []mysql.Column, ptrs []interface{}) error {
	for i, c := range cols {
		switch c.Name {
		case "String":
			src := []byte(*(ptrs[i].(*sql.RawBytes)))
			if len(src) == 0 {
				break
			}

			z.String = string(src)
		}
	}

	return nil
}

func TestRowScanner(t *testing.T) {
	var v myString
	err := coolDB.Select(&v, "select 'hello!'`String`", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, "hello!", v.String)
}

func TestString(t *testing.T) {
	var v string
	err := coolDB.Select(&v, "select 'hello!'", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, "hello!", v)
}

func TestNullString(t *testing.T) {
	var v string
	err := coolDB.Select(&v, "select null", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, "", v)
}

func TestStringPtr(t *testing.T) {
	var v *string
	err := coolDB.Select(&v, "select 'hello!'", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, "hello!", *v)
}

func TestNullStringPtr(t *testing.T) {
	var v *string
	err := coolDB.Select(&v, "select null", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, (*string)(nil), v)
}

func TestInt(t *testing.T) {
	var v int
	err := coolDB.Select(&v, "select 7", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, 7, v)
}

func TestNullInt(t *testing.T) {
	var v int
	err := coolDB.Select(&v, "select null", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, 0, v)
}

func TestIntPtr(t *testing.T) {
	var v *int
	err := coolDB.Select(&v, "select 7", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, 7, *v)
}

func TestNullIntPtr(t *testing.T) {
	var v *int
	err := coolDB.Select(&v, "select null", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, (*int)(nil), v)
}

func TestScan(t *testing.T) {
	var v sql.NullString
	err := coolDB.Select(&v, "select 'hello!'", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, "hello!", v.String)
}

func TestNullScan(t *testing.T) {
	var v sql.NullString
	err := coolDB.Select(&v, "select null", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, false, v.Valid)
}

func TestGenericStruct(t *testing.T) {
	var v struct {
		Hello  string
		Number int
	}
	err := coolDB.Select(&v, "select 'hello!'`Hello`,7`Number`", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, struct {
		Hello  string
		Number int
	}{"hello!", 7}, v)
}

func TestGenericStructDifferentOrder(t *testing.T) {
	var v struct {
		Number int
		World  string
		Hello  string
	}
	err := coolDB.Select(&v, "select 'hello!'`Hello`,7`Number`,'not used!'", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, struct {
		Number int
		World  string
		Hello  string
	}{7, "", "hello!"}, v)
}

func TestGenericStructNamedField(t *testing.T) {
	var v struct {
		Hello  string `mysql:"bye"`
		Number int
	}
	err := coolDB.Select(&v, "select 'hello!'`Hello`,7`Number`,'bye!'`bye`", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, struct {
		Hello  string `mysql:"bye"`
		Number int
	}{"bye!", 7}, v)
}

func TestGenericStructJSON(t *testing.T) {
	var v struct {
		Object map[string]string
		Ints   []int
	}
	err := coolDB.Select(&v, "select '{\"hello\": \"world\"}'`Object`,'[1, 2, 3]'`Ints`", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, struct {
		Object map[string]string
		Ints   []int
	}{map[string]string{"hello": "world"}, []int{1, 2, 3}}, v)
}

func TestIntSlice(t *testing.T) {
	var v []int
	err := coolDB.Select(&v, "select 7 union select 2", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, []int{7, 2}, v)
}

func TestIntChan(t *testing.T) {
	v := make(chan int)
	err := coolDB.Select(v, "select 7 union select 2", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, 7, <-v)
	Equal(t, 2, <-v)
}

// sadly it's not possible to detect the difference between uint8 and byte *at all*
// func TestUint8Slice(t *testing.T) {
// 	var v []uint8
// 	err := coolDB.Select(&v, "select 7 union select 2", 0)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	Equal(t, []uint8{7, 2}, v)
// }

func TestByteSlice(t *testing.T) {
	var v []byte
	err := coolDB.Select(&v, "select 0x001234", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, []byte{0x0, 0x12, 0x34}, v)
}

func TestByteSliceSlice(t *testing.T) {
	var v [][]byte
	err := coolDB.Select(&v, "select 0x001234 union select 0x005678", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, [][]byte{{0x0, 0x12, 0x34}, {0x0, 0x56, 0x78}}, v)
}

func TestTime(t *testing.T) {
	var v time.Time
	err := coolDB.Select(&v, "select curdate()", 0)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, time.Now().Truncate(24*time.Hour).UTC().Format(time.RFC3339Nano), v.UTC().Format(time.RFC3339Nano))
}

func TestTimeSlice(t *testing.T) {
	var v []time.Time
	err := coolDB.Select(&v, "select curdate() union select curdate()+interval 1 day", 0)
	if err != nil {
		t.Fatal(err)
	}

	for i, vv := range v {
		switch i {
		case 0:
			Equal(t, time.Now().Truncate(24*time.Hour).UTC().Format(time.RFC3339Nano), vv.UTC().Format(time.RFC3339Nano))
		case 1:
			Equal(t, time.Now().Truncate(24*time.Hour).Add(24*time.Hour).UTC().Format(time.RFC3339Nano), vv.UTC().Format(time.RFC3339Nano))
		}
	}
}

func TestStringCached(t *testing.T) {
	var v string
	err := coolDB.Select(&v, "select uuid()", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var v2 string
	err = coolDB.Select(&v2, "select uuid()", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, v, v2)

	time.Sleep(2 * time.Second)

	err = coolDB.Select(&v2, "select uuid()", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	NotEqual(t, v, v2)
}

func TestGenericStructDifferentOrderCached(t *testing.T) {
	var v struct {
		Number int
		World  string
		Hello  string
	}
	err := coolDB.Select(&v, "select 'hello!'`Hello`,7`Number`,'not used!'", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, struct {
		Number int
		World  string
		Hello  string
	}{7, "", "hello!"}, v)

	err = coolDB.Select(&v, "select 'hello!'`Hello`,7`Number`,'not used!'", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, struct {
		Number int
		World  string
		Hello  string
	}{7, "", "hello!"}, v)
}

func TestGenericStructNamedFieldCached(t *testing.T) {
	var v struct {
		Hello  string `mysql:"bye"`
		Number int
	}
	err := coolDB.Select(&v, "select 'hello!'`Hello`,7`Number`,'bye!'`bye`", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, struct {
		Hello  string `mysql:"bye"`
		Number int
	}{"bye!", 7}, v)

	err = coolDB.Select(&v, "select 'hello!'`Hello`,7`Number`,'bye!'`bye`", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	Equal(t, struct {
		Hello  string `mysql:"bye"`
		Number int
	}{"bye!", 7}, v)
}
