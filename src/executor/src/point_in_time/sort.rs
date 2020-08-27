use crate::point_in_time::BoxedExecutor;
use crate::scalar_expression::EvalScalar;
use crate::utils::right_size_new_to;
use crate::ExecutionError;
use ast::expr::SortExpression;
use data::encoding_core::SortableEncoding;
use data::{Datum, Session, SortOrder, TupleIter};
use std::sync::Arc;
use std::vec::IntoIter;

/// An executor that sorts expressions based on some sort expression(s).
pub struct SortExecutor {
    source: BoxedExecutor,
    session: Arc<Session>,
    sort_expressions: Vec<SortExpression>,
    sort_buffer: Vec<u8>,
    // start/end
    sort_indexes: IntoIter<(u32, u32)>,
    state: State,
    tuple_buffer: Vec<Datum<'static>>,
    freq: i64,
}

#[derive(Eq, PartialEq)]
enum State {
    Ready,
    Serving,
    Done,
}

// Implementation..
// The naive approach is storing tuples in a vec/btree and then sorting that. but..
// this has a couple of downsides
// 1. We have to .static() all the datums/tuples *and* allow vecs for each tuple
//    resulting in allot of allocations.
// 2. Each datum takes up 24 bytes in memory, while serialized datums might average
//    something more like 5 bytes resulting in 5x less memory (or 5x more rows
//    before we have to spill to disk).
//
// What we'll do instead serialize the tuples(preceded by the evaluated sort expressions)
// into one big buffer and keep a set of pointers into the buffer and then we'll sort
// the pointers.
// Due to our buffer potentially resizing and reallocating our pointers can't be slices,
// but rather integers, we'll choose u32 for now which will limit our sorts to 4gb of
// data we'd want to spill to disk and implement external sort way before then

impl SortExecutor {
    pub fn new(
        session: Arc<Session>,
        source: BoxedExecutor,
        sort_expressions: Vec<SortExpression>,
    ) -> Self {
        let tuple_buffer = right_size_new_to(source.column_count());
        SortExecutor {
            source,
            session,
            sort_expressions,
            sort_buffer: vec![],
            sort_indexes: vec![].into_iter(),
            state: State::Ready,
            tuple_buffer,
            freq: 0,
        }
    }
}

impl TupleIter for SortExecutor {
    type E = ExecutionError;

    fn advance(&mut self) -> Result<(), ExecutionError> {
        if self.state == State::Ready {
            self.ingest()?;
            self.state = State::Serving
        }

        if let Some((start, end)) = self.sort_indexes.next() {
            let mut slice = &self.sort_buffer[(start as usize)..(end as usize)];
            let mut sort_datum = Datum::Null;
            // First Ingest/throw away the sort keys.
            for _ in 0..self.sort_expressions.len() {
                slice = sort_datum.from_sortable_bytes(slice);
            }
            for datum in &mut self.tuple_buffer {
                slice = datum.from_sortable_bytes(slice);
            }
            self.freq.read_sortable_bytes(SortOrder::Asc, slice);
        } else {
            // Free up all our memory here.
            self.sort_buffer = vec![];
            self.sort_indexes = vec![].into_iter();
            self.state = State::Done
        }

        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        if self.state == State::Done {
            None
        } else {
            Some((&self.tuple_buffer, self.freq))
        }
    }

    fn column_count(&self) -> usize {
        self.source.column_count()
    }
}

impl SortExecutor {
    /// Ingests all the tuples from the source and sorts the buffer.
    fn ingest(&mut self) -> Result<(), ExecutionError> {
        // Try and size our buffers big enough initially that malloc will
        // mmap and be able to grow via realloc without memcopy'ing.
        self.sort_buffer = Vec::with_capacity(128 * 1024 * 1024);
        let mut sort_indexes =
            Vec::with_capacity(128 * 1024 * 1024 / std::mem::size_of::<(u32, u32)>());

        while let Some((tuple, freq)) = self.source.next()? {
            let start = self.sort_buffer.len() as u32;

            for sort_expr in &mut self.sort_expressions {
                let datum = sort_expr.expression.eval_scalar(&self.session, tuple);
                datum.as_sortable_bytes(sort_expr.ordering, &mut self.sort_buffer);
            }

            for datum in tuple {
                datum.as_sortable_bytes(SortOrder::Asc, &mut self.sort_buffer);
            }
            freq.write_sortable_bytes(SortOrder::Asc, &mut self.sort_buffer);

            if self.sort_buffer.len() > u32::MAX as usize {
                panic!("Oversized sort, external sort not yet implemented");
            }
            let end = self.sort_buffer.len() as u32;
            sort_indexes.push((start, end));
        }

        let sort_buffer = &mut self.sort_buffer;
        sort_indexes.sort_unstable_by(|(start1, end1), (start2, end2)| {
            let a = &sort_buffer[(*start1 as usize)..(*end1 as usize)];
            let b = &sort_buffer[(*start2 as usize)..(*end2 as usize)];
            a.cmp(b)
        });

        self.sort_indexes = sort_indexes.into_iter();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::point_in_time::values::ValuesExecutor;
    use ast::expr::{CompiledColumnReference, Expression};
    use data::DataType;

    #[test]
    fn test_sort_executor() -> Result<(), ExecutionError> {
        let session = Arc::new(Session::new(1));
        let values = vec![
            vec![Datum::from(1), Datum::from("a")],
            vec![Datum::from(1), Datum::from("b")],
            vec![Datum::from(2), Datum::from("c")],
        ];

        let source = Box::from(ValuesExecutor::new(Box::from(values.into_iter()), 2));

        let mut executor = SortExecutor::new(
            session,
            source,
            vec![
                SortExpression {
                    ordering: SortOrder::Desc,
                    expression: Expression::CompiledColumnReference(CompiledColumnReference {
                        offset: 0,
                        datatype: DataType::Integer,
                    }),
                },
                SortExpression {
                    ordering: SortOrder::Asc,
                    expression: Expression::CompiledColumnReference(CompiledColumnReference {
                        offset: 1,
                        datatype: DataType::Text,
                    }),
                },
            ],
        );

        assert_eq!(
            executor.next()?,
            Some(([Datum::from(2), Datum::from("c")].as_ref(), 1))
        );
        assert_eq!(
            executor.next()?,
            Some(([Datum::from(1), Datum::from("a")].as_ref(), 1))
        );
        assert_eq!(
            executor.next()?,
            Some(([Datum::from(1), Datum::from("b")].as_ref(), 1))
        );
        assert_eq!(executor.next()?, None);

        Ok(())
    }
}
