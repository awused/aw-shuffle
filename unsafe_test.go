package strpick

import (
	"reflect"
	"testing"
)

// Unsafe is just a thin wrapper around internal.Base
// just ensure everything functions
func TestUnsafeFunctionality(t *testing.T) {
	u := NewUnsafePicker()

	verifySize(t, u, 0)
	err := u.Add("a")
	verifyError(t, err, nil)
	verifySize(t, u, 1)
	ss, err := u.Values()
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a"})
	err = u.AddAll([]string{"a", "b", "c"})
	verifyError(t, err, nil)
	verifySize(t, u, 3)
	ss, err = u.Values()
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a", "b", "c"})
	err = u.Remove("b")
	verifyError(t, err, nil)
	verifySize(t, u, 2)
	err = u.RemoveAll([]string{"b", "c"})
	verifyError(t, err, nil)
	verifySize(t, u, 1)

	s, err := u.Next()
	verifyError(t, err, nil)
	verifyString(t, s, "a")
	ss, err = u.NextN(3)
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a", "a", "a"})
	ss, err = u.UniqueN(1)
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a"})
	ss, err = u.TryUniqueN(1)
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a"})
	ss, err = u.TryUniqueN(3)
	verifyError(t, err, nil)
	verifyStrings(t, ss, []string{"a", "a", "a"})

	verifyError(t, u.SetBias(123), nil)

	err = u.Close()
	verifyError(t, err, nil)

	_, err = u.Size()
	verifyError(t, err, ErrClosed)
}

func verifySize(t *testing.T, p Picker, size int) {
	if s, err := p.Size(); err != nil || s != size {
		t.Errorf("Unexpected values returned by Size(), got [%d, %v] expected [%d]",
			s, err, size)
	}
}

func verifyError(t *testing.T, err error, expected error) {
	if err != expected {
		t.Errorf(
			"Expected error [%s] not thrown, got [%s] instead", expected, err)
	}
}

func verifyString(t *testing.T, s string, expected string) {
	if s != expected {
		t.Errorf(
			"Expected string [%s] not returned, got [%s] instead", expected, s)
	}
}

func verifyStrings(t *testing.T, ss []string, expected []string) {
	if !reflect.DeepEqual(ss, expected) {
		t.Errorf(
			"Expected strings %v not returned, got %v instead", expected, ss)
	}
}
