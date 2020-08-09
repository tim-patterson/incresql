use data::{DataType, Datum};
use runtime::Runtime;

/// Test helper that creates a new runtime/connection, submits the queries and
/// compares the resultset with the expected
pub fn query(query: &str, expected: &str) {
    let runtime = Runtime::new();
    let connection = runtime.new_connection();
    let (fields, mut executor) = connection.execute_statement(query).unwrap();
    let types: Vec<_> = fields.iter().map(|f| f.data_type).collect();
    let mut rows: Vec<String> = vec![];
    while let Some((tuple, freq)) = executor.next().unwrap() {
        for _ in 0..freq {
            let row = tuple.iter().enumerate().map(|(idx, value)|
                // For decimal we want to grab the precision for formatting
                if let Datum::Decimal(d) = value {
                    if let DataType::Decimal(_, scale) = types[idx] {
                        format!("{:.*}", scale as usize, d)
                    } else {
                        panic!()
                    }
                } else {
                    value.to_string()
                }
            ).collect::<Vec<_>>().join("|");

            rows.push(row);
        }
    }

    let expected_rows: Vec<_> = expected
        .split("\n")
        .filter_map(|row| {
            let trimmed = row.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .collect();

    for ((idx, actual), expected) in rows.iter().enumerate().zip(expected_rows.iter()) {
        if actual != expected {
            panic!(
                "actual != expected @ line {}\n  actual={}\nexpected={}\n",
                idx + 1,
                actual,
                expected
            );
        }
    }
    if rows.len() > expected_rows.len() {
        panic!(
            "actual has {} more rows than expected",
            rows.len() - expected_rows.len()
        );
    }

    if rows.len() < expected_rows.len() {
        panic!(
            "actual has {} less rows than expected",
            expected_rows.len() - rows.len()
        );
    }
}