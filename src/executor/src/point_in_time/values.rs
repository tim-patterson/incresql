use crate::point_in_time::Executor;
use crate::ExecutionError;
use data::Datum;

pub struct ValuesExecutor {
    iter: Box<dyn Iterator<Item = Vec<Datum<'static>>>>,
    curr_row: Option<Vec<Datum<'static>>>,
    column_count: usize,
}

impl ValuesExecutor {
    pub fn new(iter: Box<dyn Iterator<Item = Vec<Datum<'static>>>>, column_count: usize) -> Self {
        ValuesExecutor {
            iter,
            curr_row: None,
            column_count,
        }
    }
}

impl Executor for ValuesExecutor {
    fn advance(&mut self) -> Result<(), ExecutionError> {
        self.curr_row = self.iter.next();
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i32)> {
        self.curr_row.as_ref().map(|row| (row.as_ref(), 1))
    }

    fn column_count(&self) -> usize {
        self.column_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecutionError;

    #[test]
    fn test_values_executor() -> Result<(), ExecutionError> {
        let values = vec![
            vec![Datum::from(1), Datum::from("1")],
            vec![Datum::from(2), Datum::from("2")],
        ];

        let mut executor = ValuesExecutor::new(Box::from(values.into_iter()), 2);

        assert_eq!(executor.column_count(), 2);

        assert_eq!(
            executor.next()?,
            Some(([Datum::from(1), Datum::from("1")].as_ref(), 1))
        );

        assert_eq!(
            executor.next()?,
            Some(([Datum::from(2), Datum::from("2")].as_ref(), 1))
        );

        assert_eq!(executor.next()?, None);
        Ok(())
    }
}
