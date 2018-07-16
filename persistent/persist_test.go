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
	store := storage.NewMemStorage()
	db, err := leveldb.Open(store, nil)
	if err != nil {
		t.Fatal(err)
	}

	p := &persist{b: internal.NewBasePicker(), m: &sync.Mutex{}, db: db}

	verifyNilError(t, p.Add("a"))
	has, err := db.Has([]byte("a"), nil)
	if !has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, true)
	}
	verifyNilError(t, p.AddAll([]string{"b", "c"}))
	has, err = db.Has([]byte("b"), nil)
	if !has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, true)
	}
	has, err = db.Has([]byte("c"), nil)
	if !has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, true)
	}

	verifyNilError(t, p.Remove("c"))
	has, err = db.Has([]byte("c"), nil)
	if has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, false)
	}
	has, err = db.Has([]byte("b"), nil)
	if !has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, true)
	}

	verifyNilError(t, p.RemoveAll([]string{"b", "a"}))
	has, err = db.Has([]byte("a"), nil)
	if has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, false)
	}
	has, err = db.Has([]byte("b"), nil)
	if has || err != nil {
		t.Errorf("Unexpected values returned from has, got [%t, %v] expected "+
			"[%t, nil]", has, err, false)
	}

	verifyNilError(t, p.Close())
}

func TestWritesToDB_Next(t *testing.T) {
	store := storage.NewMemStorage()
	db, err := leveldb.Open(store, nil)
	if err != nil {
		t.Fatal(err)
	}

	p := &persist{b: internal.NewLeftmostOldestBasePicker(), m: &sync.Mutex{}, db: db}

	verifyNilError(t, p.Add("a"))
	verifyNilError(t, p.Add("b"))
	olda, err := db.Get([]byte("a"), nil)
	oldb, err := db.Get([]byte("b"), nil)
	verifyNilError(t, err)

	s, err := p.Next()
	if s != "a" {
		t.Fatalf("Next() was not a")
	}
	verifyNilError(t, err)
	newa, err := db.Get([]byte("a"), nil)
	verifyNilError(t, err)
	newb, err := db.Get([]byte("b"), nil)
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
	newa, err = db.Get([]byte("a"), nil)
	verifyNilError(t, err)
	newb, err = db.Get([]byte("b"), nil)
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
	newa, err = db.Get([]byte("a"), nil)
	verifyNilError(t, err)
	newb, err = db.Get([]byte("b"), nil)
	verifyNilError(t, err)

	if reflect.DeepEqual(olda, newa) {
		t.Error("UniqueN() did not change the value for a in the database")
	}
	if reflect.DeepEqual(oldb, newb) {
		t.Error("UniqueN() did not change the value for b in the database")
	}
}

func TestReadsFromDB_AddAddAll(t *testing.T) {
	store := storage.NewMemStorage()
	db, err := leveldb.Open(store, nil)
	if err != nil {
		t.Fatal(err)
	}

	p := &persist{b: internal.NewLeftmostOldestBasePicker(), m: &sync.Mutex{}, db: db}

	verifyNilError(t, p.AddAll([]string{"a", "b", "c"}))
	s, err := p.Next() // Reads "a"
	verifyNilError(t, err)

	p = &persist{b: internal.NewLeftmostOldestBasePicker(), m: &sync.Mutex{}, db: db}

	verifyNilError(t, p.Add("a"))
	verifyNilError(t, p.Add("b"))
	s, err = p.Next() // Reads "b" because "a" has been more recently selected
	verifyNilError(t, err)
	if s != "b" {
		t.Fatalf("Next() was not b")
	}

}

func verifyNilError(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}
