package strpick

import (
	"sync"

	"github.com/awused/go-strpick/internal"
)

/**
The default picker, safe for use from multiple goroutines.
*/
type picker struct {
	b *internal.Base
	m *sync.Mutex
}

func NewPicker() Picker {
	return &picker{b: internal.NewBasePicker(), m: &sync.Mutex{}}
}

func (t *picker) Add(s string) error {
	t.m.Lock()
	_, _, err := t.b.Add(s)
	t.m.Unlock()
	return err
}
func (t *picker) AddAll(ss []string) error {
	t.m.Lock()
	_, _, err := t.b.AddAll(ss)
	t.m.Unlock()
	return err
}

func (t *picker) Remove(s string) error {
	t.m.Lock()
	_, err := t.b.Remove(s)
	t.m.Unlock()
	return err
}
func (t *picker) RemoveAll(ss []string) error {
	t.m.Lock()
	_, err := t.b.RemoveAll(ss)
	t.m.Unlock()
	return err
}

func (t *picker) Next() (string, error) {
	t.m.Lock()
	s, _, err := t.b.Next()
	t.m.Unlock()
	return s, err
}
func (t *picker) NextN(n int) ([]string, error) {
	t.m.Lock()
	ss, _, err := t.b.NextN(n)
	t.m.Unlock()
	return ss, err
}
func (t *picker) UniqueN(n int) ([]string, error) {
	t.m.Lock()
	ss, _, err := t.b.UniqueN(n)
	t.m.Unlock()
	return ss, err
}

func (t *picker) Size() (int, error) {
	t.m.Lock()
	sz, err := t.b.Size()
	t.m.Unlock()
	return sz, err
}
func (t *picker) Values() ([]string, error) {
	t.m.Lock()
	ss, err := t.b.Values()
	t.m.Unlock()
	return ss, err
}

func (t *picker) Close() error {
	t.m.Lock()
	err := t.b.Close()
	t.m.Unlock()
	return err
}
