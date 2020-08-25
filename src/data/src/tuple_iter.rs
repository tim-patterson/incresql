use crate::Datum;
use serde::export::PhantomData;

/// Essentially a streaming iterator specialized for tuples/freqs
pub trait TupleIter {
    type E;
    /// Advance the iterator to the next position, should be called before get for a new iter
    fn advance(&mut self) -> Result<(), Self::E>;

    /// Get the data at the current position of the iterator, the i64 is a frequency/
    fn get(&self) -> Option<(&[Datum], i64)>;

    /// Short cut function that calls advance followed by get.
    fn next(&mut self) -> Result<Option<(&[Datum], i64)>, Self::E> {
        self.advance()?;
        Ok(self.get())
    }

    /// Returns the count of columns from this iter. Used to help size buffers etc
    fn column_count(&self) -> usize;
}

pub fn empty_tuple_iter<E: 'static>() -> Box<dyn TupleIter<E = E>> {
    Box::from(EmptyTupleIter {
        _p: PhantomData::default(),
    })
}

struct EmptyTupleIter<E> {
    _p: PhantomData<E>,
}

impl<E> TupleIter for EmptyTupleIter<E> {
    type E = E;
    fn advance(&mut self) -> Result<(), Self::E> {
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        None
    }

    fn column_count(&self) -> usize {
        0
    }
}

/// A wrapper that allows us to peek at the next value but still be able to
/// later call next normally and get the same value that we just peeked at.
pub struct PeekableIter<I: TupleIter + ?Sized> {
    inner: Box<I>,
    advanced: bool,
}

impl<I: TupleIter + ?Sized> PeekableIter<I> {
    /// Return the next value or None if there is no next value
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(&[Datum], i64)>, I::E> {
        if !self.advanced {
            self.inner.advance()?;
        }

        let next = self.inner.get();

        // This allows us to call peek after the iterator is exhausted
        // without doing anything weird...
        // the binding's into rocks db can segfault if using the mem env
        // for example... but other iterators in general may do weird
        // undefined behaviour things...
        if next.is_some() {
            self.advanced = false;
        } else {
            self.advanced = true;
        }

        Ok(next)
    }

    /// Peeks at the next value with out mucking up the state.
    pub fn peek(&mut self) -> Result<Option<(&[Datum], i64)>, I::E> {
        if !self.advanced {
            self.inner.advance()?;
            self.advanced = true;
        }
        Ok(self.inner.get())
    }

    /// If we've peeked at a value and want to act like we'd
    /// actually called next, instead of calling next you can
    /// call this instead and not have to worry about consuming
    /// the output
    pub fn lock_in(&mut self) {
        self.advanced = false;
    }

    pub fn column_count(&self) -> usize {
        self.inner.column_count()
    }
}

impl<E> From<Box<dyn TupleIter<E = E>>> for PeekableIter<dyn TupleIter<E = E>> {
    fn from(inner: Box<dyn TupleIter<E = E>>) -> Self {
        PeekableIter {
            inner,
            advanced: false,
        }
    }
}
