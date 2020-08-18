use runtime::connection::Connection;
use runtime::Runtime;

/// Creates a new connection and passes it to the closure
pub fn with_connection<F: FnOnce(&Connection)>(f: F) {
    let runtime = Runtime::new_for_test();
    let connection = runtime.new_connection();
    f(&connection)
}

/// Test helper that creates a new runtime/connection and executes a single query
pub fn query(query: &str, expected: &str) {
    with_connection(|connection| connection.query(query, expected))
}

pub trait TestQuery {
    fn query(&self, query: &str, expected: &str);
}

impl TestQuery for Connection<'_> {
    fn query(&self, query: &str, expected: &str) {
        let (fields, mut executor) = self.execute_statement(query).unwrap();
        let types: Vec<_> = fields.iter().map(|f| f.data_type).collect();
        let mut rows: Vec<String> = vec![];
        while let Some((tuple, freq)) = executor.next().unwrap() {
            for _ in 0..freq {
                let row = tuple
                    .iter()
                    .enumerate()
                    .map(|(idx, value)| value.typed_with(types[idx]).to_string())
                    .collect::<Vec<_>>()
                    .join("|");

                rows.push(format!("|{}|", row));
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
                    "actual != expected @ line {}\n  actual={}\nexpected={}\n\nactual_rows:\n{}\n\nexpected_rows:\n{}\n",
                    idx + 1,
                    actual,
                    expected,
                    rows.join("\n"),
                    expected_rows.join("\n")
                );
            }
        }
        if rows.len() > expected_rows.len() {
            panic!(
                "actual has {} more rows than expected\n\nactual_rows:\n{}\n\nexpected_rows:\n{}\n",
                rows.len() - expected_rows.len(),
                rows.join("\n"),
                expected_rows.join("\n")
            );
        }

        if rows.len() < expected_rows.len() {
            panic!(
                "actual has {} less rows than expected\n\nactual_rows:\n{}\n\nexpected_rows:\n{}\n",
                expected_rows.len() - rows.len(),
                rows.join("\n"),
                expected_rows.join("\n")
            );
        }
    }
}
