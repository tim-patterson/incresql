use crate::point_in_time::Executor;
use crate::ExecutionError;
use data::Datum;
use std::cmp::min;

pub struct LimitExecutor {
    source: Box<dyn Executor>,
    offset_remaining: i64,
    limit_remaining: i64,
    current_freq: i32,
}

impl LimitExecutor {
    pub fn new(source: Box<dyn Executor>, offset: i64, limit: i64) -> Self {
        LimitExecutor {
            source,
            offset_remaining: offset,
            limit_remaining: limit,
            current_freq: 0,
        }
    }
}

impl Executor for LimitExecutor {
    fn advance(&mut self) -> Result<(), ExecutionError> {
        while self.offset_remaining > 0 {
            if let Some((_tuple, freq)) = self.source.next()? {
                self.offset_remaining -= freq as i64;
                if self.offset_remaining < 0 {
                    self.current_freq = -self.offset_remaining as i32;
                    return Ok(());
                }
            } else {
                break;
            }
        }

        if self.limit_remaining > 0 {
            if let Some((_tuple, freq)) = self.source.next()? {
                self.current_freq = min(freq as i64, self.limit_remaining) as i32;
                self.limit_remaining -= freq as i64;
                return Ok(());
            }
        }
        self.current_freq = 0;

        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i32)> {
        if self.current_freq != 0 {
            self.source
                .get()
                .map(|(tuple, _freq)| (tuple, self.current_freq))
        } else {
            None
        }
    }

    fn column_count(&self) -> usize {
        self.source.column_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::point_in_time::values::ValuesExecutor;

    #[test]
    fn test_limit_executor() -> Result<(), ExecutionError> {
        let values = vec![
            vec![Datum::from(1)],
            vec![Datum::from(2)],
            vec![Datum::from(3)],
        ];

        let source = Box::from(ValuesExecutor::new(Box::from(values.into_iter()), 1));

        let mut executor = LimitExecutor::new(source, 1, 1);

        assert_eq!(executor.next()?, Some(([Datum::from(2)].as_ref(), 1)));
        assert_eq!(executor.next()?, None);

        Ok(())
    }
}
