use rust_decimal::Decimal;

/// Datum - in memory representation of sql value.
#[derive(Clone, Debug)]
pub enum Datum<'a> {
    Null,
    Boolean(bool),
    // Text type from on-disk tuple, just points back to the rocks db key/value bytes
    TextRef(&'a str),
    // On-heap text type, potentially used for function return types or where we need a static
    // lifetime, ie select max(str_col)
    TextOwned(Box<str>),
    // Inline text type, optimization of TextOwned where the text is small enough to store inline
    // without having pay the cost of allocation/pointer chasing.
    TextInline(u8, [u8; 22]),

    // Bytes type from on-disk tuple, just points back to the rocks db key/value bytes
    BytesRef(&'a [u8]),
    // On-heap bytes type, potentially used for function return types or where we need a static
    // lifetime, ie select max(bytes_col)
    BytesOwned(Box<[u8]>),
    // Inline bytes type, optimization of BytesOwned where the data is small enough to store inline
    // without having pay the cost of allocation/pointer chasing.
    BytesInline(u8, [u8; 22]),

    Integer(i64),
    Decimal(Decimal),
}

impl Default for Datum<'_> {
    fn default() -> Self {
        Datum::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn test_datum_size() {
        // The decimal and &str types need to be at least 2 words aka 16bytes wide,
        // When we include the discriminator (1 byte) that makes it 17 bytes, however due to
        // word sized alignment for the &str pointers we actually end up at 24 bytes in size,
        // this means we've got enough room for 23 byte of data for short strings etc.
        assert_eq!(24, size_of::<Datum>());
    }
}
