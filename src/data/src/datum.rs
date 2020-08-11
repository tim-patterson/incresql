use crate::DECIMAL_MAX_SCALE;
use rust_decimal::Decimal;
use std::fmt::{Debug, Display, Formatter};

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

impl<'a> Datum<'a> {
    /// Like clone but instead of cloning Datum::TextOwned etc it will just take a reference
    pub fn ref_clone(&'a self) -> Datum<'a> {
        if let Datum::TextOwned(s) = self {
            Datum::TextRef(&s)
        } else {
            self.clone()
        }
    }

    /// As datums can reference data external to themselves they're only guaranteed to be valid
    /// for the current iteration of the iterator/loop etc. This method creates a new datum with
    /// any borrowed data now owned so it can be held onto across iterations(ie to sort them).
    pub fn as_static(&'a self) -> Datum<'static> {
        match self {
            Datum::Null => Datum::Null,
            Datum::Boolean(b) => Datum::Boolean(*b),
            Datum::Integer(i) => Datum::Integer(*i),
            Datum::BigInt(i) => Datum::BigInt(*i),
            Datum::Decimal(d) => Datum::Decimal(*d),
            Datum::TextOwned(s) => Datum::TextOwned(s.clone()),
            Datum::TextInline(l, bytes) => Datum::TextInline(*l, *bytes),
            Datum::TextRef(s) => {
                let len = s.len();
                if len <= 22 {
                    let mut bytes = [0_u8; 22];
                    bytes.as_mut()[..len].copy_from_slice(s.as_bytes());
                    Datum::TextInline(len as u8, bytes)
                } else {
                    Datum::TextOwned(Box::from(*s))
                }
            }
        }
    }

    /// Returns true if this value is null
    pub fn is_null(&self) -> bool {
        if let Datum::Null = self {
            true
        } else {
            false
        }
    }

    /// Returns true if this value is equal to another.
    /// According to sql rules, null != null, this is the behaviour if null_safe = false,
    /// if null_safe is set to true then null == null
    pub fn sql_eq(&self, other: &Self, null_safe: bool) -> bool {
        match self {
            Datum::Null => other.is_null() && null_safe,
            Datum::Boolean(b) => other.as_boolean() == Some(*b),
            Datum::Integer(i) => other.as_integer() == Some(*i),
            Datum::BigInt(i) => other.as_bigint() == Some(*i),
            Datum::Decimal(d) => other.as_decimal() == Some(*d),
            Datum::TextOwned(_) | Datum::TextInline(..) | Datum::TextRef(_) => {
                self.as_str() == other.as_str()
            }
        }
    }
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
    fn from(mut d: Decimal) -> Self {
        if d.scale() > DECIMAL_MAX_SCALE as u32 {
            d.rescale(DECIMAL_MAX_SCALE as u32);
        }
        Datum::Decimal(d)
    }
}

impl From<String> for Datum<'static> {
    fn from(s: String) -> Self {
        Datum::TextOwned(s.into_boxed_str())
    }
}

impl<'a> From<&'a str> for Datum<'a> {
    fn from(s: &'a str) -> Self {
        Datum::TextRef(s)
    }
}

impl Display for Datum<'_> {
    /// When used with the alternate flag this will format as a sql string, ie strings will be quoted
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Datum::Null => f.write_str("NULL"),
            Datum::TextRef(_) | Datum::TextOwned(_) | Datum::TextInline(..) => {
                let str = self.as_str().unwrap();
                if f.alternate() {
                    // The debug trait should quote and escape our strings for us
                    Debug::fmt(str, f)
                } else {
                    f.write_str(str)
                }
            }
            Datum::Boolean(b) => f.write_str(if *b { "TRUE" } else { "FALSE" }),
            Datum::Integer(i) => Display::fmt(i, f),
            Datum::BigInt(i) => Display::fmt(i, f),
            Datum::Decimal(d) => Display::fmt(d, f),
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

    pub fn as_boolean(&self) -> Option<bool> {
        if let Datum::Boolean(b) = self {
            Some(*b)
        } else {
            None
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
    fn test_datum_ref_clone() {
        assert_eq!(Datum::from(1).ref_clone(), Datum::Integer(1));

        assert_eq!(
            Datum::TextOwned("hello".to_string().into_boxed_str()).ref_clone(),
            Datum::TextRef("hello")
        );
    }

    #[test]
    fn test_datum_as_static() {
        assert_eq!(
            Datum::TextRef("Hello world").as_static(),
            Datum::TextInline(11, *b"Hello world\0\0\0\0\0\0\0\0\0\0\0")
        );

        assert_eq!(
            Datum::TextRef("Hello world123456789123456789").as_static(),
            Datum::TextOwned(Box::from("Hello world123456789123456789"))
        );
    }

    #[test]
    fn test_datum_is_null() {
        assert_eq!(Datum::Null.is_null(), true);

        assert_eq!(Datum::from(1).is_null(), false);
    }

    #[test]
    fn test_datum_sql_eq() {
        // Nulls
        assert_eq!(Datum::Null.sql_eq(&Datum::Null, false), false);
        assert_eq!(Datum::Null.sql_eq(&Datum::Null, true), true);
        // Mixed Nulls with bools
        assert_eq!(Datum::from(true).sql_eq(&Datum::Null, true), false);
        assert_eq!(Datum::Null.sql_eq(&Datum::from(true), true), false);
        assert_eq!(Datum::from(false).sql_eq(&Datum::from(false), false), true);
        // Strings
        assert_eq!(Datum::from("abc").sql_eq(&Datum::from("abc"), false), true);
        assert_eq!(Datum::from("abc").sql_eq(&Datum::from("efg"), false), false);
        assert_eq!(
            Datum::from("abc").sql_eq(&Datum::TextOwned(Box::from("abc")), false),
            true
        );
        assert_eq!(
            Datum::TextOwned(Box::from("abc")).sql_eq(&Datum::from("abc"), false),
            true
        );
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

        assert_eq!(Datum::from("Hello world"), Datum::TextRef("Hello world"));
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
    fn test_datum_as_boolean() {
        assert_eq!(Datum::Boolean(true).as_boolean(), Some(true));

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
        assert_eq!(
            format!("{:#}", Datum::from("he\"llo".to_string())),
            "\"he\\\"llo\""
        );
    }
}
