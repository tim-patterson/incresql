use crate::runner::query;

#[test]
fn select_predicate_constant() {
    query(
        r#"SELECT 1 where true"#,
        "
        |1|
        ",
    );

    query(
        r#"EXPLAIN SELECT 1 where true"#,
        "
        |PROJECT|||
        | |exprs:|||
        | |  _col1 <INTEGER>|0|1|
        | |source:|||
        | |  FILTER|||
        | |   |predicate:||TRUE|
        | |   |source:|||
        | |   |  SINGLE|||
        ",
    );
}

#[test]
fn select_predicate_reference() {
    query(
        r#"SELECT foo from (select 1 as foo) where foo = 1"#,
        "
        |1|
        ",
    );

    query(
        r#"EXPLAIN SELECT foo from (select 1 as foo) where foo = 1"#,
        "
        |PROJECT|||
        | |exprs:|||
        | |  foo <INTEGER>|0|<OFFSET 0>|
        | |source:|||
        | |  FILTER|||
        | |   |predicate:||`=`(<OFFSET 0>, 1)|
        | |   |source:|||
        | |   |  PROJECT|||
        | |   |   |exprs:|||
        | |   |   |  foo <INTEGER>|0|1|
        | |   |   |source:|||
        | |   |   |  SINGLE|||
        ",
    );
}
