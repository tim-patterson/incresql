use data::Datum;

/// Initializes a buffer(vector) to the same size as the passed in vector and returns it.
/// Fills the buffer with the default values
pub(crate) fn right_size_new<T: Default, Y>(from: &[Y]) -> Vec<T> {
    from.iter().map(|_| T::default()).collect()
}

/// Initializes a buffer(vector) to the passed in size
/// Fills the buffer with the default values
pub fn right_size_new_to<T: Default>(size: usize) -> Vec<T> {
    (0..size).map(|_| T::default()).collect()
}

/// Used to transmute a datum buffer from static to 'a so we can insert data into it
pub(crate) fn transmute_muf_buf<'a>(buf: &'a mut [Datum<'static>]) -> &'a mut [Datum<'a>] {
    unsafe {
        #[allow(clippy::transmute_ptr_to_ptr)]
        std::mem::transmute::<&mut [Datum<'static>], &mut [Datum<'_>]>(buf)
    }
}

/// Used to transmute a datum buffer from static to 'a for reading and safe use downstream
pub(crate) fn transmute_buf<'a>(buf: &'a [Datum<'static>]) -> &'a [Datum<'a>] {
    unsafe {
        #[allow(clippy::transmute_ptr_to_ptr)]
        std::mem::transmute::<&[Datum<'static>], &[Datum<'_>]>(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_right_size_new() {
        let from = vec![1, 2, 3, 4, 5];
        let to: Vec<bool> = right_size_new(&from);

        assert_eq!(to, vec![false, false, false, false, false])
    }

    #[test]
    fn test_right_size_new_to() {
        let to: Vec<bool> = right_size_new_to(5);

        assert_eq!(to, vec![false, false, false, false, false])
    }
}
