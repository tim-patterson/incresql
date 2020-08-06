# Incresql Source

The incresql source is divided into many internal crates, this helps us keep the code clean by
using the compiler to help us enforce modularity and encapsulation between the different parts of incresql.

## Crates
* **ast** - Contains AST nodes for rel and expressions
* **data** - Contains Datum structures and their related serialization code.
* **executor** - Code that actually runs the plans from the planner.
* **functions** - Contains functions used in expressions
* **parser** - Contains parser
* **planner** - Validates the parsed sql and optimizes and plans how to then execute
* **runtime** - Responsible for coordinating/scheduling everything around the lifecycle of a session.