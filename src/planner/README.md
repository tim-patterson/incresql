# Planner crate

The planner crate takes the raw ast from the parser and massages it into a shape that we can hand
off to the executor for efficient evaluation.

To do this the query passes through a number of phases

1. *Validation* - This is the phase where we resolve column aliases, table names, function names etc and perform
type checking.
Some amount of normalization is done in this phase too, creating column aliases where they weren't specified and
other misc operations.
If we're going to throw a planning error it should be in this phase.

2. *Optimization* - In this phase we move things around a bit to perform general high level optimizations, this would
be things like predicate pushdowns and constant foldings

3. *Execution Planning* - In this phase we take the plan and perform any execution specific optimizations and convert
from the logical operator tree to the physical.