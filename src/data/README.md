# Data crate

The data crate contains Datum structures and their related serialization code.

### Datums
A datum is simply the in memory representation of sql value, ie a string, number etc, and a row is simply a list of datums.

A datum is self describing, ie it's a tagged union (enum in rust speak) however the mapping between sql type and datum
may not be 1-1, ie we have both a TextRef, a TextOwned and a TextInline
