// Package strpick implements a random selection algorithm that favours less
// recently selected items.
package strpick

import "github.com/awused/go-strpick/internal"

var (
	ErrClosed             = internal.ErrClosed
	ErrEmpty              = internal.ErrEmpty
	ErrCorrupt            = internal.ErrCorrupt
	ErrOverflow           = internal.ErrOverflow
	ErrNegative           = internal.ErrNegative
	ErrNaN                = internal.ErrNaN
	ErrInsufficientUnique = internal.ErrInsufficientUnique
)

// Picker is a efficient randomized selector that operates on strings.
type Picker interface {
	// Add inserts a string into the Picker. The newly added string will be
	// treated the same as the current least-recently picked string.
	// The time complexity is O(log(n)).
	Add(string) error
	// AddAll inserts multiple strings into the Picker. The newly added strings
	// will be treated the same as the current least-recently picked string.
	// The time complexity is O(m*log(n)), where m is the number of strings to be
	// added.
	AddAll([]string) error

	// Remove deletes a string from the picker in O(log(n)) time.
	Remove(string) error
	// RemoveAll deletes strings from the picker in O(m*log(n)) time.
	RemoveAll([]string) error

	// Next randomly picks a string, favouring less recently selected strings.
	// The time complexity is O(log(n)).
	Next() (string, error)
	// NextN randomly picks N strings, favouring less recently selected strings.
	// The returned strings will all be treated by subsequent calls as having
	// been selected at the same time.
	// It is possible for the same string to be returned multiple times.
	// The time complexity is O(N*log(n)).
	NextN(int) ([]string, error)
	// UniqueN randomly picks N unique strings, favouring less recently selected
	// strings.
	// The returned strings will all be treated by subsequent calls as having
	// been selected at the same time.
	// It is an error to call UniqueN with an N larger than the number of strings
	// in the picker.
	// The time complexity is O(N*log(n)).
	UniqueN(int) ([]string, error)
	// TryUniqueN conditionally calls UniqueN or NextN depending on whether there
	// are enough strings present to guarantee unique results.
	TryUniqueN(int) ([]string, error)

	// SetBias controls how strongly the picker biases towards older values.
	// Bias must be non-negative. Larger values for bias will cause the picker to
	// return older strings more often. A bias of 0 causes the picker to ignore
	// how recently strings have been selected, making all strings equally likely
	// to be selected. A bias of +Inf will result in the picker exclusively
	// selecting the least-recently selected strings. The default bias is 2.
	SetBias(float64) error

	// SetRandomlyDistributeNewStrings changes the behaviour of newly added
	// strings from being always considered as if they have not ever been picked
	// to giving them a random generation so they're less likely to be picked.
	SetRandomlyDistributeNewStrings(rand bool) error

	// Size returns the number of strings currently present in the picker.
	Size() (int, error)
	// Values returns all strings in the picker in lexicographical order.
	Values() ([]string, error)

	// Close closes the picker. It is not necessary to call this on
	// non-persistent pickers. Calling any methods on a closed picker is an
	// error.
	Close() error
}
