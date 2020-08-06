# Planner crate

The planner crate takes the raw ast from the parser.

It then:
1. Validates the sql using information from the catalog if needed.
2. Normalizes the query, filling in missing column aliases etc
2. Performs needed transforms, ie rewriting distincts into group bys, rewriting `*` etc
3. Figures out how to build up a plan to actually run the query.