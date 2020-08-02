# Incresql Source

The incresql source is divided into many internal crates, this helps us keep the code clean by
using the compiler to help us enforce modularity and encapsulation between the different parts of incresql.

## Crates
* **ast** - Contains AST nodes for rel and expressions
* **data** - Contains Datum structures and their related serialization code.
* **functions** - Contains functions used in expressions
* **parser** - Contains parser
