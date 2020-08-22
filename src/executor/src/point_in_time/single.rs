use crate::ExecutionError;
use data::{Datum, TupleIter};

pub struct SingleExecutor {
    state: State,
}

enum State {
    Ready,
    Single,
    Done,
}

impl SingleExecutor {
    pub fn new() -> Self {
        SingleExecutor {
            state: State::Ready,
        }
    }
}

impl TupleIter for SingleExecutor {
    type E = ExecutionError;

    fn advance(&mut self) -> Result<(), ExecutionError> {
        self.state = match self.state {
            State::Ready => State::Single,
            State::Single => State::Done,
            State::Done => State::Done,
        };
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        if let State::Single = self.state {
            Some((&[], 1))
        } else {
            None
        }
    }

    fn column_count(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_executor() -> Result<(), ExecutionError> {
        let mut executor = SingleExecutor::new();
        assert_eq!(executor.next()?, Some((&[] as &[Datum], 1)));
        assert_eq!(executor.next()?, None);
        Ok(())
    }
}
