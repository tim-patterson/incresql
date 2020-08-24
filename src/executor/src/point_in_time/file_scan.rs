use crate::ExecutionError;
use data::json::{JsonBuilder, OwnedJson};
use data::{Datum, TupleIter};
use std::iter::{empty, once};
use std::path::PathBuf;

/// Walks all the files in the directory reads them in as json.
pub struct FileScanExecutor {
    lines: Box<dyn Iterator<Item = Result<OwnedJson, ExecutionError>>>,
    tuple: [Datum<'static>; 1],
    done: bool,
}

impl FileScanExecutor {
    pub fn new(directory: String) -> Self {
        let file_entries = entries(PathBuf::from(directory));

        FileScanExecutor {
            lines: Box::from(file_entries.flat_map(csv_lines)),
            tuple: [Datum::Null; 1],
            done: false,
        }
    }
}

impl TupleIter for FileScanExecutor {
    type E = ExecutionError;

    fn advance(&mut self) -> Result<(), Self::E> {
        if let Some(next) = self.lines.next() {
            let line = next?;
            self.tuple[0] = Datum::from(line);
        } else {
            self.done = true;
        }
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        if self.done {
            None
        } else {
            Some((&self.tuple, 1))
        }
    }

    fn column_count(&self) -> usize {
        1
    }
}

/// Returns a flattened iterator of all the files within a director
/// horrible unwrapping and rewrapping of result types
fn entries(entry: PathBuf) -> Box<dyn Iterator<Item = Result<PathBuf, std::io::Error>>> {
    if entry.is_file() {
        Box::from(once(Ok(entry)))
    } else if entry.is_dir() {
        match entry.read_dir() {
            Ok(iter) => Box::from(iter.flat_map(|entry_result| match entry_result {
                Ok(entry) => entries(entry.path()),
                Err(err) => Box::from(once(Err(err))),
            })),
            Err(e) => Box::from(once(Err(e))),
        }
    } else {
        Box::from(empty())
    }
}

// // Returns all the lines from a file.
// fn lines(
//     entry: Result<PathBuf, std::io::Error>,
// ) -> Box<dyn Iterator<Item = Result<String, std::io::Error>>> {
//     match entry {
//         Ok(entry) => {
//             let file = File::open(entry);
//             match file {
//                 Ok(file) => Box::from(BufReader::new(file).lines()),
//                 Err(e) => Box::from(once(Err(e))),
//             }
//         }
//         Err(e) => Box::from(once(Err(e))),
//     }
// }

fn csv_lines(
    entry: Result<PathBuf, std::io::Error>,
) -> Box<dyn Iterator<Item = Result<OwnedJson, ExecutionError>>> {
    match entry {
        Ok(entry) => {
            let mut builder = csv::ReaderBuilder::new();
            builder.has_headers(false);
            let reader_result = builder.from_path(entry);
            match reader_result {
                Ok(reader) => Box::from(reader.into_records().map(|record_result| {
                    record_result
                        .map(|record| {
                            JsonBuilder::default().array(|array| {
                                for col in record.iter() {
                                    array.push_string(col);
                                }
                            })
                        })
                        .map_err(ExecutionError::from)
                })),
                Err(e) => Box::from(once(Err(e.into()))),
            }
        }
        Err(e) => Box::from(once(Err(e.into()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_lines() -> Result<(), ExecutionError> {
        let path = PathBuf::from("../../test_data/csv/simple.csv");

        let mut line_iter = csv_lines(Ok(path));

        let expected_line1 = OwnedJson::parse(r#"["123","abc","12.1"]"#).unwrap();
        let expected_line2 = OwnedJson::parse(r#"["456","d,ef","13.2"]"#).unwrap();

        assert_eq!(line_iter.next().unwrap().unwrap(), expected_line1);
        assert_eq!(line_iter.next().unwrap().unwrap(), expected_line2);
        assert_eq!(line_iter.next(), None);

        Ok(())
    }

    #[test]
    fn test_single_csv() -> Result<(), ExecutionError> {
        let directory = "../../test_data/csv/simple.csv".to_string();

        let mut executor = FileScanExecutor::new(directory);

        let expected_line1 = OwnedJson::parse(r#"["123","abc","12.1"]"#).unwrap();
        let expected_line2 = OwnedJson::parse(r#"["456","d,ef","13.2"]"#).unwrap();

        assert_eq!(
            executor.next()?,
            Some(([Datum::from(expected_line1)].as_ref(), 1))
        );
        assert_eq!(
            executor.next()?,
            Some(([Datum::from(expected_line2)].as_ref(), 1))
        );
        assert_eq!(executor.next()?, None);

        Ok(())
    }

    #[test]
    fn test_csv_director() -> Result<(), ExecutionError> {
        let directory = "../../test_data/csv".to_string();

        let mut executor = FileScanExecutor::new(directory);

        let expected_line1 = OwnedJson::parse(r#"["123","abc","12.1"]"#).unwrap();

        // Lets just test the first line
        assert_eq!(
            executor.next()?,
            Some(([Datum::from(expected_line1)].as_ref(), 1))
        );

        Ok(())
    }
}
