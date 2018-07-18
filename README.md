go-strpick
=========

An efficient random string picker that favours less recently selected strings. Supports live insertions and deletions, and optional persistence using leveldb.

Seeks to be generic enough that it can be used for very different behaviours. All implementations here except UnsafePicker are thread safe. UnsafePicker can be used as long as it's only accessed from a single goroutine.


All operations on individual items are O(log n).

Memory overhead: O(n), not counting the strings themselves

# Basic Usage

```go
picker := strpick.NewPicker()

picker.AddAll(mystrings)

s, _ := picker.Next()
tenStrings, _ := picker.NextN(10)
fiveUniqueStrings, _ := picker.UniqueN(5)

picker.Close()
```

<!-- to change the weighting of the randomizer to control how heavily it favours older selections. Must output an integer in [0, range]. range may be 0 -->
<!-- Higher values means selecting older items, returning range will mean one of the oldest items is always selected. -->
<!--  -->
<!-- by default it's range - math.Round(range * rand()**2) -->
<!--  -->
<!--  -->
<!--  -->
<!-- SetNextGenerationFunc(func(chosen string, oldestGeneration int, youngestGeneration int): int) -->
<!-- Changes the method by which new generations are assigned. -->
<!--  -->
<!-- By default it's youngestGeneration + 1. Note that youngestGeneration >= oldestGeneration -->
<!--  -->
<!--  -->
<!--  -->
<!-- SetDefaultGenerationFunc(func(newEntry string, minGen int, maxGen int): int) -->
<!-- Changes the strategy for assigning generations to newly inserted items. The PersistentPicker will favour stored values over calling this. -->
<!--  -->
<!-- By default it returns oldestGeneration. Note that youngestGeneration >= oldestGeneration -->
<!-- By default it's always 0. -->
<!--  -->
<!-- Note that youngest is always greater than or equal to oldest. -->


### Persistent Pickers

Persistent randomizers store their state in a leveldb database. This is done synchronously alongside random selections, insertions, and deletions.

When new strings are inserted into a PersistentPicker the randomizer looks up the values in the database, preferring to use a stored value over generating a new one.

PersistentPicker supports a CleanDB() function that synchronously purge invalid keys from the database. It's probably not necessary to use this as Remove() also removes the key from the database.

The current persistence implementation is too simple, and it's recommended to initialize persistent pickers with AddAll() or LoadDB() over individual Add() calls.

<!-- TODO AsyncPersistentPicker -->

```go
persist, _ := persistent.NewPicker("/path/to/database")

// OPTIONAL -- load all data from the database
// Individual values are loaded when calling Add() and AddAll()
_ = persist.LoadDB()

_ = persist.RemoveAll(unwantedStrings)

// Also runs a compaction, making this potentially very slow
_ = persist.CleanDB()

persist.Close()
```

## Closing

Use `Close()` to safely close persistent pickers. Calling any methods on a closed picker is an error.

## API

SetBias(bias float64)
Controls how strongly the picker avoids recently selected items. `bias` must be non-negative and larger values of bias cause the picker to bias itself more heavily towards older items.

A bias of 0 causes the picker to effectively ignore the age of the items, making all items equally likely to be selected, while +Inf will result in the picker exclusively picking items of the oldest generation. The default bias is 2.

<!-- SetRandomFunc(func(range int): int) -->

Next()
Randomly selects the next string

NextN(n int)
Chooses n entries that may not be unique, and assigns them all the same generation. 

UniqueN(n int)
Chooses n unique strings or returns an error if Size() < n.

Add(s string)
AddAll(ss []string)
Adds strings to the picker, if they're already present this does nothing

Remove(s string)
RemoveAll(ss []string)
Removes strings, if they're not present this does nothing

Size()
Returns the current number of strings

Values() []string
Returns every value, in order, in the tree. This is the only O(n) operation.

<!-- TODO Link to docs -->

# How It Works

Builds an in-memory red-black interval tree and tracks the recency of each item by assigning each one a generation.

To select the next item it randomly selects a number between the maximum and minimum generations, picks a random index in the tree, and searches forward until it finds the first item with a generation older than that random number.


# Performance

# Limitations

This library heavily biases towards picking less recently picked strings. This library doesn't provide any guarantees of fairness and is not suitable for applications where such guarantees are required. The design goal was to give fast results without risking integer overflows when handling many items over many generations.

Int is used internally and pickers detect but don't handle integer overflows. If an ErrOverflow is returned nothing can be done, at this point in time, except recreating the picker. This will probably never be a concern for users on 64 bit platforms so I've elected not to put much effort into handling it, at least for now.

