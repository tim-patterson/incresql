use crate::DataType;
use rust_decimal::Decimal;
use std::fmt::{Display, Formatter};

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
    Integer(i32),
    BigInt(i64),
    Decimal(Decimal),
}

// From builders to build datums from the native rust types
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

impl From<i32> for Datum<'static> {
    fn from(i: i32) -> Self {
        Datum::Integer(i)
    }
}

impl From<i64> for Datum<'static> {
    fn from(i: i64) -> Self {
        Datum::BigInt(i)
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

impl Display for Datum<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Datum::Null => f.write_str("NULL"),
            Datum::TextRef(_) | Datum::TextOwned(_) | Datum::TextInline(..) => {
                f.write_str(self.as_str().unwrap())
            }
            Datum::Boolean(b) => f.write_str(if *b { "TRUE" } else { "FALSE" }),
            Datum::Integer(i) => i.fmt(f),
            Datum::BigInt(i) => i.fmt(f),
            Datum::Decimal(d) => d.fmt(f),
        }
    }
}

// Into's to get back rust types from datums, these are just "dumb" and simply map 1-1 without any
// attempts to do any casting
impl<'a> Datum<'a> {
    pub fn as_str(&'a self) -> Option<&'a str> {
        match self {
            Datum::TextRef(s) => Some(s),
            Datum::TextInline(len, b) => {
                Some(unsafe { std::str::from_utf8_unchecked(&b.as_ref()[..(*len as usize)]) })
            }
            Datum::TextOwned(s) => Some(s.as_ref()),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i32> {
        if let Datum::Integer(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_bigint(&self) -> Option<i64> {
        if let Datum::BigInt(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_decimal(&self) -> Option<Decimal> {
        if let Datum::Decimal(d) = self {
            Some(*d)
        } else {
            None
        }
    }
}

impl Datum<'_> {
    pub fn datatype(&self) -> DataType {
        match self {
            Datum::Null => DataType::Null,
            Datum::Boolean(_) => DataType::Boolean,
            Datum::Integer(_) => DataType::Integer,
            Datum::BigInt(_) => DataType::BigInt,
            Datum::Decimal(d) => DataType::Decimal(28, d.scale() as u8),
            Datum::TextOwned(_) | Datum::TextInline(..) | Datum::TextRef(_) => DataType::Text,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
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
    fn test_datum_from_bigint() {
        assert_eq!(Datum::from(1234_i64), Datum::BigInt(1234));
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

    #[test]
    fn test_datum_as_str() {
        assert_eq!(
            Datum::TextOwned(Box::from("Hello world")).as_str(),
            Some("Hello world")
        );

        let mut bytes = [0_u8; 22];
        bytes.as_mut().write_all("Hello world".as_bytes()).unwrap();

        assert_eq!(Datum::TextInline(11, bytes).as_str(), Some("Hello world"));

        let backing_slice = "Hello world";
        assert_eq!(Datum::TextRef(backing_slice).as_str(), Some("Hello world"));

        assert_eq!(Datum::Null.as_str(), None);
    }

    #[test]
    fn test_datum_as_ints() {
        assert_eq!(Datum::Integer(123).as_integer(), Some(123_i32));

        assert_eq!(Datum::Null.as_integer(), None);

        assert_eq!(Datum::BigInt(123).as_bigint(), Some(123_i64));

        assert_eq!(Datum::Null.as_bigint(), None);
    }

    #[test]
    fn test_datum_as_decimal() {
        assert_eq!(
            Datum::Decimal(Decimal::new(3232, 1)).as_decimal(),
            Some(Decimal::new(3232, 1))
        );

        assert_eq!(Datum::Null.as_decimal(), None);
    }

    #[test]
    fn test_datum_display() {
        assert_eq!(format!("{}", Datum::Null), "NULL");

        assert_eq!(format!("{}", Datum::Boolean(true)), "TRUE");

        assert_eq!(format!("{}", Datum::Integer(123)), "123");
        assert_eq!(format!("{}", Datum::BigInt(123)), "123");

        assert_eq!(
            format!("{}", Datum::Decimal(Decimal::from_str("12.34").unwrap())),
            "12.34"
        );

        assert_eq!(format!("{}", Datum::from("hello".to_string())), "hello");
    }
}
