/// Initializes a buffer(vector) to the same size as the passed in vector and returns it.
/// Fills the buffer with the default values
pub fn right_size_new<T: Default, Y>(from: &[Y]) -> Vec<T> {
    from.iter().map(|_| T::default()).collect()
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
}
