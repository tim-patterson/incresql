// /// Initializes a passed in buffer(vector) to the same size as the passed in vector.
// /// Fills the buffer with the default values
pub fn right_size<T: Default, Y>(buffer: &mut Vec<T>, from: &[Y]) {
    if buffer.len() != from.len() {
        *buffer = from.iter().map(|_| T::default()).collect();
    }
}

/// Initializes a buffer(vector) to the same size as the passed in vector and returns it.
/// Fills the buffer with the default values
pub fn right_size_new<T: Default, Y>(from: &[Y]) -> Vec<T> {
    from.iter().map(|_| T::default()).collect()
}

// /// Initializes a buffer(vector) to the passed in size
// /// Fills the buffer with the default values
// pub fn right_size_new_to<T: Default>(size: usize) -> Vec<T> {
//     (0..size).map(|_| T::default()).collect()
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_right_size() {
        let from = vec![1, 2, 3, 4, 5];
        let mut to: Vec<bool> = Vec::new();
        right_size(&mut to, &from);

        assert_eq!(to, vec![false, false, false, false, false])
    }

    #[test]
    fn test_right_size_new() {
        let from = vec![1, 2, 3, 4, 5];
        let to: Vec<bool> = right_size_new(&from);

        assert_eq!(to, vec![false, false, false, false, false])
    }

    // #[test]
    // fn test_right_size_new_to() {
    //     let to: Vec<bool> = right_size_new_to(5);
    //
    //     assert_eq!(to, vec![false, false, false, false, false])
    // }
}
