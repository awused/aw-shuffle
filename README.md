go-strpick
=========

An efficient random string picker that favours less recently selected strings. Supports live insertions and deletions, and offers optional persistence using leveldb.

Seeks to be generic enough that it can be used for very different behaviours. All implementations here except UnsafePicker are thread safe. UnsafePicker can be used as long as it's only accessed from a single goroutine at a time.

All operations on individual items are have O(log n) time complexity.

Memory overhead: O(n), not counting the strings themselves

# Usage

See the [documentation](http://godoc.org/github.com/awused/go-strpick)

```go
picker := strpick.NewPicker()
defer picker.Close()

picker.AddAll(mystrings)

s, _ := picker.Next()
tenStrings, _ := picker.NextN(10)
fiveUniqueStrings, _ := picker.UniqueN(5)
```

## Persistent Pickers

Persistent randomizers store their state in a leveldb database. This is done synchronously and transparently when calling regular methods. When new strings are inserted into a PersistentPicker the randomizer looks up how recently selected the item was from the database.

PersistentPicker supports a CleanDB() function that synchronously purges invalid keys from the database. It's probably not necessary to use this very often as Remove() also removes the key from the database.


```go
persist, _ := persistent.NewPicker("/path/to/database")
defer persist.Close()

// OPTIONAL -- load all data from the database
// Individual values are loaded when calling Add() and AddAll()
_ = persist.LoadDB()

// Keep unwantedStrings in the database for the future
_ = persist.SoftRemoveAll(unwantedStrings)

// Deletes any strings not currently in persist from the database, including unwantedStrings
_ = persist.CleanDB()
```

## Closing

Use `Close()` to safely close persistent pickers. Calling any methods on a closed picker is an error. Closing non-persistent pickers is optional but encouraged.

# How It Works

Builds an in-memory red-black interval tree and tracks the recency of each item by assigning each one a generation.

To select the next item it randomly selects a number between the maximum and minimum generations, picks a random index in the tree, and searches forward until it finds the first item with a generation older than that random number. This very strongly biases the selector towards less recently selected items.

## Performance

All operations on individual strings are performed in O(log(n)) time, including selections, insertions, and deletions. 

For persistent pickers all database reads and writes are performed synchronously, but batching is used where appropriate to attempt to limit the impact of operations on many strings.

## Limitations

All strings need to be kept in memory, for comparison purposes, and there are currently 80 bytes of additional overhead per string.

This library does heavily bias towards picking less recently picked strings, but not in a way that is easy to define mathematically. This library doesn't provide any guarantees of fairness and does not try to be random in a way that is useful for cryptography or generating passwords. The design goal was to give fast results without risking integer overflows when handling many items over many generations.

Int is used internally and pickers detect but don't handle integer overflows. If an ErrOverflow is returned nothing can be done, at this point in time, except recreating the picker. This will probably never be a concern for users on 64 bit platforms so I've elected not to put much effort into handling it, at least for now.

