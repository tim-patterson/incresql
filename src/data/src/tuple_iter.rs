use crate::Datum;

/// Essentially a streaming iterator specialized for tuples/freqs
pub trait TupleIter<E> {
    /// Advance the iterator to the next position, should be called before get for a new iter
    fn advance(&mut self) -> Result<(), E>;

    /// Get the data at the current position of the iterator, the i64 is a frequency/
    fn get(&self) -> Option<(&[Datum], i64)>;

    /// Short cut function that calls advance followed by get.
    fn next(&mut self) -> Result<Option<(&[Datum], i64)>, E> {
        self.advance()?;
        Ok(self.get())
    }

    /// Returns the count of columns from this iter. Used to help size buffers etc
    fn column_count(&self) -> usize;
}

impl<E> dyn TupleIter<E> {
    pub fn empty() -> Box<dyn TupleIter<E>> {
        Box::from(EmptyTupleIter {})
    }
}

struct EmptyTupleIter {}

impl<E> TupleIter<E> for EmptyTupleIter {
    fn advance(&mut self) -> Result<(), E> {
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        None
    }

    fn column_count(&self) -> usize {
        0
    }
}
