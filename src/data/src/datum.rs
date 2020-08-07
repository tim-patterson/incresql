use crate::DataType;
use rust_decimal::Decimal;

/// Datum - in memory representation of sql value.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    Integer(i64),
    Decimal(Decimal),
}

impl Default for Datum<'_> {
    fn default() -> Self {
        Datum::Null
    }
}

impl From<bool> for Datum<'static> {
    fn from(b: bool) -> Self {
        Datum::Boolean(b)
    }
}

impl From<i64> for Datum<'static> {
    fn from(i: i64) -> Self {
        Datum::Integer(i)
    }
}

impl From<Decimal> for Datum<'static> {
    fn from(d: Decimal) -> Self {
        Datum::Decimal(d)
    }
}

impl From<String> for Datum<'static> {
    fn from(d: String) -> Self {
        Datum::TextOwned(d.into_boxed_str())
    }
}

impl Datum<'_> {
    pub fn datatype(&self) -> DataType {
        match self {
            Datum::Null => DataType::Null,
            Datum::Boolean(_) => DataType::Boolean,
            Datum::Integer(_) => DataType::Integer,
            Datum::Decimal(d) => DataType::Decimal(28, d.scale() as u8),
            Datum::TextOwned(_) | Datum::TextInline(..) | Datum::TextRef(_) => DataType::Text,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;
    use std::str::FromStr;

    #[test]
    fn test_datum_size() {
        // The decimal and &str types need to be at least 2 words aka 16bytes wide,
        // When we include the discriminator (1 byte) that makes it 17 bytes, however due to
        // word sized alignment for the &str pointers we actually end up at 24 bytes in size,
        // this means we've got enough room for 23 byte of data for short strings etc.
        assert_eq!(24, size_of::<Datum>());
    }

    #[test]
    fn test_datum_from_boolean() {
        assert_eq!(Datum::from(true), Datum::Boolean(true));
        assert_eq!(Datum::from(false), Datum::Boolean(false));
    }

    #[test]
    fn test_datum_from_integer() {
        assert_eq!(Datum::from(1234), Datum::Integer(1234));
    }

    #[test]
    fn test_datum_from_decimal() {
        assert_eq!(
            Datum::from(Decimal::new(12345, 2)),
            Datum::Decimal(Decimal::new(12345, 2))
        );
    }

    #[test]
    fn test_datum_from_string() {
        assert_eq!(
            Datum::from(String::from("Hello world")),
            Datum::TextOwned(Box::from("Hello world"))
        );
    }

    #[test]
    fn test_datum_datatype() {
        assert_eq!(Datum::Null.datatype(), DataType::Null);

        assert_eq!(
            Datum::from(String::from("Hello world")).datatype(),
            DataType::Text
        );

        assert_eq!(Datum::from(1).datatype(), DataType::Integer);

        assert_eq!(Datum::from(false).datatype(), DataType::Boolean);
    }

    #[test]
    fn test_datum_datatype_decimal() {
        assert_eq!(
            Datum::from(Decimal::from_str("123").unwrap()).datatype(),
            DataType::Decimal(28, 0)
        );

        assert_eq!(
            Datum::from(Decimal::from_str("-123").unwrap()).datatype(),
            DataType::Decimal(28, 0)
        );

        assert_eq!(
            Datum::from(Decimal::from_str("123.12").unwrap()).datatype(),
            DataType::Decimal(28, 2)
        );

        assert_eq!(
            Datum::from(Decimal::from_str("-123.12").unwrap()).datatype(),
            DataType::Decimal(28, 2)
        );

        assert_eq!(
            Datum::from(Decimal::from_str("123.00").unwrap()).datatype(),
            DataType::Decimal(28, 2)
        );
    }
}
