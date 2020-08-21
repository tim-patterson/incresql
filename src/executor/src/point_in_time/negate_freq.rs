use crate::point_in_time::BoxedExecutor;
use crate::ExecutionError;
use data::{Datum, TupleIter};

/// An executor that simply negates the frequencies of tuples
/// passing through. This used for deletes where a delete is
/// simply implemented as an insert with negated freq's.
pub struct NegateFreqExecutor {
    source: BoxedExecutor,
}

impl NegateFreqExecutor {
    pub fn new(source: BoxedExecutor) -> Self {
        NegateFreqExecutor { source }
    }
}

impl TupleIter for NegateFreqExecutor {
    type E = ExecutionError;

    fn advance(&mut self) -> Result<(), ExecutionError> {
        self.source.advance()
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        self.source.get().map(|(tuple, freq)| (tuple, -freq))
    }

    fn column_count(&self) -> usize {
        self.source.column_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::point_in_time::single::SingleExecutor;

    #[test]
    fn test_project_executor() -> Result<(), ExecutionError> {
        let source = SingleExecutor::new();

        let mut executor = NegateFreqExecutor::new(Box::from(source));

        assert_eq!(executor.next()?, Some(([].as_ref(), -1)));
        Ok(())
    }
}
