package strpick

import "github.com/awused/go-strpick/internal"

/**
A Picker with no locking. Unsafe to use concurrently from multiple goroutines.

Returns errors if it ever detects it has entered an inconsistent state
as a result of concurrent access, but does not try to reliably detect misuse.
*/
type unsafe struct {
	b *internal.Base
}

// NewUnsafePicker returns a picker with no locking that is not thread-safe,
// but can be used from a single thread. It may return ErrCorrupt if it
// detects that it is in an inconsistent state, but does not attempt to
// proactively detect parallel access.
func NewUnsafePicker() Picker {
	return &unsafe{b: internal.NewBasePicker()}
}

func (t *unsafe) Add(s string) error {
	_, _, err := t.b.Add(s)
	return err
}
func (t *unsafe) AddAll(ss []string) error {
	_, _, err := t.b.AddAll(ss)
	return err
}

func (t *unsafe) Remove(s string) error {
	_, err := t.b.Remove(s)
	return err
}
func (t *unsafe) RemoveAll(ss []string) error {
	_, err := t.b.RemoveAll(ss)
	return err
}

func (t *unsafe) Next() (string, error) {
	s, _, err := t.b.Next()
	return s, err
}
func (t *unsafe) NextN(n int) ([]string, error) {
	ss, _, err := t.b.NextN(n)
	return ss, err
}
func (t *unsafe) UniqueN(n int) ([]string, error) {
	ss, _, err := t.b.UniqueN(n)
	return ss, err
}
func (t *unsafe) TryUniqueN(n int) ([]string, error) {
	ss, _, err := t.b.UniqueN(n)
	if err == ErrInsufficientUnique {
		ss, _, err = t.b.NextN(n)
	}

	return ss, err
}

func (t *unsafe) SetBias(bi float64) error {
	return t.b.SetBias(bi)
}

func (t *unsafe) Size() (int, error) {
	return t.b.Size()
}
func (t *unsafe) Values() ([]string, error) {
	return t.b.Values()
}

func (t *unsafe) Close() error {
	return t.b.Close()
}
