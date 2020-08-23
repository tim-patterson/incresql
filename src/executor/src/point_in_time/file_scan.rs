use crate::utils::right_size_new_to;
use crate::ExecutionError;
use data::{Datum, TupleIter};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::iter::{empty, once};
use std::path::PathBuf;

/// Walks all the files in the directory reads them in as json.
pub struct FileScanExecutor {
    lines: Box<dyn Iterator<Item = Result<String, std::io::Error>>>,
    line: String,
    tuple: Vec<Datum<'static>>,
}

impl FileScanExecutor {
    pub fn new(directory: String) -> Self {
        let file_entries = entries(PathBuf::from(directory));

        FileScanExecutor {
            lines: Box::from(file_entries.flat_map(lines)),
            line: String::new(),
            tuple: right_size_new_to(3),
        }
    }
}

impl TupleIter for FileScanExecutor {
    type E = ExecutionError;

    fn advance(&mut self) -> Result<(), Self::E> {
        if let Some(next) = self.lines.next() {
            let line = next?;
        }
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        unimplemented!()
    }

    fn column_count(&self) -> usize {
        3
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

/// Returns all the lines from a file.
fn lines(
    entry: Result<PathBuf, std::io::Error>,
) -> Box<dyn Iterator<Item = Result<String, std::io::Error>>> {
    match entry {
        Ok(entry) => {
            let file = File::open(entry);
            match file {
                Ok(file) => Box::from(BufReader::new(file).lines()),
                Err(e) => Box::from(once(Err(e))),
            }
        }
        Err(e) => Box::from(once(Err(e))),
    }
}
