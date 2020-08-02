use crate::point_in_time::Executor;
use data::Datum;

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

impl Executor for SingleExecutor {
    fn advance(&mut self) -> Result<(), ()> {
        self.state = match self.state {
            State::Ready => State::Single,
            State::Single => State::Done,
            State::Done => State::Done,
        };
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i32)> {
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
    fn test_single_executor() -> Result<(), ()> {
        let mut executor = SingleExecutor::new();
        assert_eq!(executor.next()?, Some((&[] as &[Datum], 1)));
        assert_eq!(executor.next()?, None);
        Ok(())
    }
}
