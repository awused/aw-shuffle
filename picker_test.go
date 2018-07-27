package strpick

import "testing"

// Picker is just a thin wrapper around internal.Base
// just ensure everything functions and locks are always released
func TestPickerFunctionality(t *testing.T) {
	p := NewPicker()

	verifySize(t, p, 0)
	err := p.Add("a")
	verifyError(t, err, nil)
	verifySize(t, p, 1)
	ss, err := p.Values()
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a"})
	err = p.AddAll([]string{"a", "b", "c"})
	verifyError(t, err, nil)
	verifySize(t, p, 3)
	ss, err = p.Values()
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a", "b", "c"})
	err = p.Remove("b")
	verifyError(t, err, nil)
	verifySize(t, p, 2)
	err = p.RemoveAll([]string{"b", "c"})
	verifyError(t, err, nil)
	verifySize(t, p, 1)

	s, err := p.Next()
	verifyError(t, err, nil)
	verifyString(t, s, "a")
	ss, err = p.NextN(3)
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a", "a", "a"})
	ss, err = p.UniqueN(1)
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a"})
	ss, err = p.TryUniqueN(1)
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a"})
	ss, err = p.TryUniqueN(3)
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a", "a", "a"})

	verifyError(t, p.SetBias(123), nil)

	err = p.Close()
	verifyError(t, err, nil)

	_, err = p.Size()
	verifyError(t, err, ErrClosed)
}
