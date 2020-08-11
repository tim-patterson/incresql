use crate::point_in_time::Executor;
use crate::ExecutionError;
use data::Datum;
use std::vec::IntoIter;

pub struct UnionAllExecutor {
    sources: IntoIter<Box<dyn Executor>>,
    curr_source: Box<dyn Executor>,
}

impl UnionAllExecutor {
    pub fn new(sources: Vec<Box<dyn Executor>>) -> Self {
        let mut sources_iter = sources.into_iter();
        let first = sources_iter
            .next()
            .expect("Union contructed with no sources!");
        UnionAllExecutor {
            sources: sources_iter,
            curr_source: first,
        }
    }
}

impl Executor for UnionAllExecutor {
    fn advance(&mut self) -> Result<(), ExecutionError> {
        // Basically a union all is just a flatmap
        loop {
            if self.curr_source.next()?.is_none() {
                if let Some(next) = self.sources.next() {
                    self.curr_source = next;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i32)> {
        self.curr_source.get()
    }

    fn column_count(&self) -> usize {
        self.curr_source.column_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::point_in_time::project::ProjectExecutor;
    use crate::point_in_time::single::SingleExecutor;
    use ast::expr::Expression;
    use data::Session;
    use std::sync::Arc;

    #[test]
    fn test_union_all_executor() -> Result<(), ExecutionError> {
        let session = Arc::from(Session::new(1));
        let sources: Vec<Box<dyn Executor>> = (0..3)
            .map(|idx| {
                let source: Box<dyn Executor> = Box::from(ProjectExecutor::new(
                    session.clone(),
                    Box::from(SingleExecutor::new()),
                    vec![Expression::from(idx)],
                ));
                source
            })
            .collect();

        let mut executor = UnionAllExecutor::new(sources);

        assert_eq!(executor.next()?, Some(([Datum::from(0)].as_ref(), 1)));
        assert_eq!(executor.next()?, Some(([Datum::from(1)].as_ref(), 1)));
        assert_eq!(executor.next()?, Some(([Datum::from(2)].as_ref(), 1)));
        assert_eq!(executor.next()?, None);

        Ok(())
    }
}
