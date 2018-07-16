package strpick

import "github.com/awused/go-strpick/internal"

var (
	ErrClosed             = internal.ErrClosed
	ErrEmpty              = internal.ErrEmpty
	ErrCorrupt            = internal.ErrCorrupt
	ErrOverflow           = internal.ErrOverflow
	ErrNegativeN          = internal.ErrNegativeN
	ErrInsufficientUnique = internal.ErrInsufficientUnique
)

type Picker interface {
	Add(string) error
	AddAll([]string) error

	Remove(string) error
	RemoveAll([]string) error

	Next() (string, error)
	NextN(int) ([]string, error)
	UniqueN(int) ([]string, error)

	Size() (int, error)
	Values() ([]string, error)

	Close() error
}
