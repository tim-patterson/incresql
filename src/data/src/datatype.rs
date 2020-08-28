use regex::Regex;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum DataType {
    // Could be considered a wildcard, ie nulls can be cast to anything
    Null,
    Boolean,
    Integer,
    BigInt,
    // Precision and scale
    Decimal(u8, u8),
    Text,
    ByteA,
    Json,
    Date,
    CompiledJsonPath,
}

pub const DECIMAL_MAX_PRECISION: u8 = 28;
pub const DECIMAL_MAX_SCALE: u8 = 14;

impl DataType {
    pub fn cast_function(&self) -> &'static str {
        match self {
            DataType::Null => panic!("Attempted cast to null"),
            DataType::Boolean => "to_bool",
            DataType::Integer => "to_int",
            DataType::BigInt => "to_bigint",
            DataType::Decimal(..) => "to_decimal",
            DataType::Text => "to_text",
            DataType::ByteA => "to_bytes",
            DataType::Json => "to_json",
            DataType::Date => "to_date",
            DataType::CompiledJsonPath => panic!(),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Null => f.write_str("NULL"),
            DataType::Boolean => f.write_str("BOOLEAN"),
            DataType::Integer => f.write_str("INTEGER"),
            DataType::BigInt => f.write_str("BIGINT"),
            DataType::Decimal(p, s) => f.write_fmt(format_args!("DECIMAL({},{})", p, s)),
            DataType::Text => f.write_str("TEXT"),
            DataType::ByteA => f.write_str("BYTEA"),
            DataType::Json => f.write_str("JSON"),
            DataType::Date => f.write_str("DATE"),
            DataType::CompiledJsonPath => f.write_str("Compiled Jsonpath"),
        }
    }
}

lazy_static! {
    static ref DECIMAL_RE: Regex = Regex::new(r"^DECIMAL\(([0-9]+),([0-9]+)\)$").unwrap();
}

/// Takes strings serialized from Display and turns them back
/// into a datatype
impl TryFrom<&str> for DataType {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "NULL" => Ok(DataType::Null),
            "BOOLEAN" => Ok(DataType::Boolean),
            "INTEGER" => Ok(DataType::Integer),
            "BIGINT" => Ok(DataType::BigInt),
            "TEXT" => Ok(DataType::Text),
            "BYTEA" => Ok(DataType::ByteA),
            "JSON" => Ok(DataType::Json),
            "DATE" => Ok(DataType::Date),
            _ => DECIMAL_RE
                .captures(value)
                .map(|d_match| {
                    let p = d_match.get(1).unwrap().as_str().parse::<u8>().unwrap();
                    let s = d_match.get(2).unwrap().as_str().parse::<u8>().unwrap();
                    DataType::Decimal(p, s)
                })
                .ok_or(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datatype_display() {
        assert_eq!(DataType::Null.to_string(), "NULL");
        assert_eq!(DataType::Decimal(1, 2).to_string(), "DECIMAL(1,2)");
    }

    #[test]
    fn test_datatype_from_str() {
        assert_eq!(DataType::try_from("NULL"), Ok(DataType::Null));
        assert_eq!(
            DataType::try_from("DECIMAL(1,2)"),
            Ok(DataType::Decimal(1, 2))
        );
    }
}
