package internal

import "errors"

var (
	ErrClosed  = errors.New("strpick: method called on closed picker")
	ErrEmpty   = errors.New("strpick: invalid Next on empty picker")
	ErrCorrupt = errors.New(
		"strpick: picker is in an invalid state, " +
			"do not use Unsafe from multiple goroutines")
	ErrOverflow = errors.New(
		"strpick: unrecoverable integer overflow, destroy and recreate the picker")
	ErrNegative           = errors.New("strpick: invalid negative number provided")
	ErrNaN                = errors.New("strpick: unexpected NaN")
	ErrInsufficientUnique = errors.New(
		"strpick: UniqueN called with N larger than Size()")
)
