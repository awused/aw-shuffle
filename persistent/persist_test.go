package persistent

import (
	"reflect"
	"sync"
	"testing"

	"github.com/awused/go-strpick/internal"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

func TestWritesToDB_AddRemove(t *testing.T) {
	db := newMemDB(t)

	p := &persist{b: internal.NewBasePicker(), m: &sync.Mutex{}, db: db}
	p.loadProperties()

	verifyNilError(t, p.Add("a"))
	has, err := db.Has([]byte("s:a"), nil)
	if !has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, true)
	}
	verifyNilError(t, p.AddAll([]string{"b", "c"}))
	has, err = db.Has([]byte("s:b"), nil)
	if !has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, true)
	}
	has, err = db.Has([]byte("s:c"), nil)
	if !has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, true)
	}

	verifyNilError(t, p.Remove("c"))
	has, err = db.Has([]byte("s:c"), nil)
	if has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, false)
	}
	has, err = db.Has([]byte("s:b"), nil)
	if !has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, true)
	}

	verifyNilError(t, p.RemoveAll([]string{"b", "a"}))
	has, err = db.Has([]byte("s:a"), nil)
	if has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, false)
	}
	has, err = db.Has([]byte("s:b"), nil)
	if has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, false)
	}

	verifyNilError(t, p.Close())
}

func TestWritesToDB_Next(t *testing.T) {
	db := newMemDB(t)
	p := newPersist(t, db)

	verifyNilError(t, p.Add("a"))
	verifyNilError(t, p.Add("b"))
	olda, err := db.Get([]byte("s:a"), nil)
	verifyNilError(t, err)
	oldb, err := db.Get([]byte("s:b"), nil)
	verifyNilError(t, err)

	s, err := p.Next()
	if s != "a" {
		t.Fatalf("Next() was not a")
	}
	verifyNilError(t, err)
	newa, err := db.Get([]byte("s:a"), nil)
	verifyNilError(t, err)
	newb, err := db.Get([]byte("s:b"), nil)
	verifyNilError(t, err)

	if reflect.DeepEqual(olda, newa) {
		t.Error("Next() did not change the value for a in the database")
	}
	if !reflect.DeepEqual(oldb, newb) {
		t.Error("Next() unexpectedly changed the value for b in the database")
	}
	olda, oldb = newa, newb

	ss, err := p.NextN(1)
	if len(ss) != 1 || ss[0] != "b" {
		t.Fatalf("NextN() was not [b]")
	}
	verifyNilError(t, err)
	newa, err = db.Get([]byte("s:a"), nil)
	verifyNilError(t, err)
	newb, err = db.Get([]byte("s:b"), nil)
	verifyNilError(t, err)

	if !reflect.DeepEqual(olda, newa) {
		t.Error("NextN() unexpectedly changed the value for a in the database")
	}
	if reflect.DeepEqual(oldb, newb) {
		t.Error("NextN() did not change the value for b in the database")
	}
	olda, oldb = newa, newb

	ss, err = p.UniqueN(2)
	if len(ss) != 2 || ss[0] != "a" || ss[1] != "b" {
		t.Fail()
	}
	verifyNilError(t, err)
	newa, err = db.Get([]byte("s:a"), nil)
	verifyNilError(t, err)
	newb, err = db.Get([]byte("s:b"), nil)
	verifyNilError(t, err)

	if reflect.DeepEqual(olda, newa) {
		t.Error("UniqueN() did not change the value for a in the database")
	}
	if reflect.DeepEqual(oldb, newb) {
		t.Error("UniqueN() did not change the value for b in the database")
	}

	ss, err = p.TryUniqueN(2)
	if len(ss) != 2 || ss[0] != "a" || ss[1] != "b" {
		t.Fail()
	}
	verifyNilError(t, err)
	newa, err = db.Get([]byte("s:a"), nil)
	verifyNilError(t, err)
	newb, err = db.Get([]byte("s:b"), nil)
	verifyNilError(t, err)

	if reflect.DeepEqual(olda, newa) {
		t.Error("TryUniqueN() did not change the value for a in the database")
	}
	if reflect.DeepEqual(oldb, newb) {
		t.Error("TryUniqueN() did not change the value for b in the database")
	}

	ss, err = p.TryUniqueN(3)
	if !reflect.DeepEqual(ss, []string{"a", "b", "a"}) {
		t.Fail()
	}
	verifyNilError(t, err)
	newa, err = db.Get([]byte("s:a"), nil)
	verifyNilError(t, err)
	newb, err = db.Get([]byte("s:b"), nil)
	verifyNilError(t, err)

	if reflect.DeepEqual(olda, newa) {
		t.Error("TryUniqueN() did not change the value for a in the database")
	}
	if reflect.DeepEqual(oldb, newb) {
		t.Error("TryUniqueN() did not change the value for b in the database")
	}
}

func TestReadsFromDB_Add(t *testing.T) {
	db := newMemDB(t)
	p := newPersist(t, db)

	verifyNilError(t, p.AddAll([]string{"a", "b", "c"}))
	// Bump all generations by one to set up the problematic test
	ss, err := p.NextN(3)
	verifyNilError(t, err)
	if !reflect.DeepEqual(ss, []string{"a", "b", "c"}) {
		t.Errorf("Unexpected response from NextN(), expected abc, got %v", ss)
	}

	_, err := p.Next() // Reads "a"
	verifyNilError(t, err)

	p = newPersist(t, db)

	verifyNilError(t, p.Add("a"))
	verifyNilError(t, p.Add("b"))
	s, err = p.Next() // Reads "b" because "a" has been more recently selected
	verifyNilError(t, err)
	if s != "b" {
		t.Fatalf("Next() was not b")
	}

	p = newPersist(t, db)
	// "d" is set to the same generation as c, not an older one
	verifyNilError(t, p.AddAll([]string{"d", "a", "b", "c"}))

	s, err = p.Next()
	verifyNilError(t, err)
	if s != "c" {
		t.Fatalf("Next() was not c")
	}

	// This part verifies that minGen is correctly persisted and loaded
	p = newPersist(t, db)

	verifyNilError(t, p.Add("e"))
	verifyNilError(t, p.AddAll([]string{"f", "g"}))
	verifyNilError(t, p.AddAll([]string{"d", "a", "b", "c"}))

	ss, err = p.NextN(4)
	verifyNilError(t, err)
	// If minGen weren't loaded, efg would have a lower generation than d and
	// this would be "efgh"
	if !reflect.DeepEqual(ss, []string{"d", "e", "f", "g"}) {
		t.Errorf("Unexpected response from NextN(), expected defg, got %v", ss)
	}
}

func TestStoresAndLoadsBias(t *testing.T) {
	db := newMemDB(t)
	p := newPersist(t, db)

	verifyNilError(t, p.AddAll([]string{"a", "b"}))

	s, err := p.Next()
	verifyNilError(t, err)
	if s != "a" {
		t.Fatalf("Next() was not a")
	}

	// 0 will effectively ignore generations and always select the smallest
	verifyNilError(t, p.SetBias(0))
	s, err = p.Next()
	verifyNilError(t, err)
	if s != "a" {
		t.Fatalf("Next() was not a")
	}

	p = newPersist(t, db)

	verifyNilError(t, p.AddAll([]string{"a", "b"}))

	s, err = p.Next()
	verifyNilError(t, err)
	if s != "a" {
		t.Fatalf("Next() was not a")
	}

	// Default
	verifyNilError(t, p.SetBias(2))

	s, err = p.Next()
	verifyNilError(t, err)
	if s != "b" {
		t.Fatalf("Next() was not b")
	}

	p = newPersist(t, db)

	verifyNilError(t, p.AddAll([]string{"a", "b"}))

	s, err = p.Next()
	verifyNilError(t, err)
	if s != "a" {
		t.Fatalf("Next() was not a")
	}
}

func TestLoadDB(t *testing.T) {
	db := newMemDB(t)
	p := newPersist(t, db)

	verifyNilError(t, p.LoadDB())
	sz, err := p.Size()
	verifyNilError(t, err)
	if sz != 0 {
		t.Fatalf("Unexpected Size() %d", sz)
	}

	_ = p.AddAll([]string{"a", "b", "c"})

	s, err := p.Next()
	verifyNilError(t, err)
	if s != "a" {
		t.Fatalf("Next() was not a")
	}

	p = newPersist(t, db)
	ss, err := p.Values()
	verifyNilError(t, err)
	if len(ss) != 0 {
		t.Fatal("Expected empty Values()")
	}

	verifyNilError(t, p.LoadDB())
	ss, err = p.Values()
	verifyNilError(t, err)
	if !reflect.DeepEqual(ss, []string{"a", "b", "c"}) {
		t.Fatalf("Got unexpected values back, expected abc got %v", ss)
	}

	s, err = p.Next()
	verifyNilError(t, err)
	if s != "b" {
		t.Fatalf("Next() was not b")
	}

}

func TestSoftRemove(t *testing.T) {
	db := newMemDB(t)
	p := newPersist(t, db)

	_ = p.AddAll([]string{"a", "b", "c", "d"})

	ss, err := p.NextN(2)
	verifyNilError(t, err)
	if !reflect.DeepEqual(ss, []string{"a", "b"}) {
		t.Fatalf("NextN(2) was not ab")
	}

	verifyNilError(t, p.SoftRemove("a"))
	ss, err = p.Values()
	if !reflect.DeepEqual(ss, []string{"b", "c", "d"}) {
		t.Fatalf("Values() was not bcd, got %v, %s", ss, err)
	}

	verifyNilError(t, p.SoftRemoveAll([]string{"a", "b", "c"}))
	ss, err = p.Values()
	if !reflect.DeepEqual(ss, []string{"d"}) {
		t.Fatalf("Values() was not d, got %v, %s", ss, err)
	}

	verifyNilError(t, p.LoadDB())

	ss, err = p.Values()
	if !reflect.DeepEqual(ss, []string{"a", "b", "c", "d"}) {
		t.Fatalf("Values() was not abcd, got %v, %s", ss, err)
	}

	ss, err = p.NextN(2)
	verifyNilError(t, err)
	if !reflect.DeepEqual(ss, []string{"c", "d"}) {
		t.Fatalf("NextN(2) was not cd")
	}
}

func TestCleanDB(t *testing.T) {
	db := newMemDB(t)
	p := newPersist(t, db)

	_ = p.SetBias(6)
	_ = p.AddAll([]string{"a", "b", "c", "d"})

	ss, err := p.NextN(4)
	verifyNilError(t, err)
	if !reflect.DeepEqual(ss, []string{"a", "b", "c", "d"}) {
		t.Fatalf("NextN(4) was not ab")
	}
	minGen := p.minGen

	err = p.SoftRemoveAll([]string{"a", "c"})
	verifyNilError(t, err)

	err = p.CleanDB()
	verifyNilError(t, err)

	newp := newPersist(t, db)
	if newp.minGen != minGen {
		t.Errorf(
			"Unexpected minGen on new tree, got %d expected %d", newp.minGen, minGen)
	}

	b, err := newp.b.GetBias()
	verifyNilError(t, err)
	if b != 6 {
		t.Errorf("Unexpected bias on new tree, got %f expected 6", b)
	}

	err = newp.LoadDB()
	verifyNilError(t, err)

	ss, err = newp.Values()
	verifyNilError(t, err)
	if !reflect.DeepEqual(ss, []string{"b", "d"}) {
		t.Fatalf("Values was not bd, got %v", ss)
	}
}

func newPersist(t *testing.T, db *leveldb.DB) *persist {
	p := &persist{
		b: internal.NewLeftmostOldestBasePicker(), m: &sync.Mutex{}, db: db}

	err := p.loadProperties()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func newMemDB(t *testing.T) *leveldb.DB {
	store := storage.NewMemStorage()
	db, err := leveldb.Open(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func verifyNilError(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}
