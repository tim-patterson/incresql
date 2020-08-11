use crate::runner::query;

#[test]
fn select_equals_nulls() {
    query(
        r#"SELECT NULL = NULL, NULL=1, 1 = NULL"#,
        "
        |NULL|NULL|NULL|
        ",
    );
}

#[test]
fn select_not_equals_nulls() {
    query(
        r#"SELECT NULL != NULL, NULL!=1, 1 != NULL"#,
        "
        |NULL|NULL|NULL|
        ",
    );
}

#[test]
fn select_equal_booleans() {
    query(
        r#"SELECT true = true, false=false, true = false, false = true"#,
        "
        |TRUE|TRUE|FALSE|FALSE|
        ",
    );
}

#[test]
fn select_not_equal_booleans() {
    query(
        r#"SELECT true != true, false!=false, true != false, false != true"#,
        "
        |FALSE|FALSE|TRUE|TRUE|
        ",
    );
}

#[test]
fn select_equal_ints() {
    query(
        r#"SELECT 1 = 1, 1=2"#,
        "
        |TRUE|FALSE|
        ",
    );
}

#[test]
fn select_not_equal_ints() {
    query(
        r#"SELECT 1 != 1, 1!=2"#,
        "
        |FALSE|TRUE|
        ",
    );
}

#[test]
fn select_equal_decimals() {
    query(
        r#"SELECT 1.0 = 1.0, 1.0=2.0, 1.2 = 1.20"#,
        "
        |TRUE|FALSE|TRUE|
        ",
    );
}

#[test]
fn select_not_equal_decimals() {
    query(
        r#"SELECT 1.0 != 1.0, 1.0!=2.0, 1.2 != 1.20"#,
        "
        |FALSE|TRUE|FALSE|
        ",
    );
}

#[test]
fn select_equal_text() {
    query(
        r#"SELECT "abc" = "abc", "abc"="ABC""#,
        "
        |TRUE|FALSE|
        ",
    );
}

#[test]
fn select_not_equal_text() {
    query(
        r#"SELECT "abc" != "abc", "abc"!="ABC""#,
        "
        |FALSE|TRUE|
        ",
    );
}
