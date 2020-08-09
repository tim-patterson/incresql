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
}

pub const DECIMAL_MAX_PRECISION: u8 = 28;
pub const DECIMAL_MAX_SCALE: u8 = 14;

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Null => f.write_str("NULL"),
            DataType::Boolean => f.write_str("BOOLEAN"),
            DataType::Integer => f.write_str("INTEGER"),
            DataType::BigInt => f.write_str("BIGINT"),
            DataType::Decimal(p, s) => f.write_fmt(format_args!("DECIMAL({},{})", p, s)),
            DataType::Text => f.write_str("TEXT"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datum_display() {
        assert_eq!(DataType::Null.to_string(), "NULL");
        assert_eq!(DataType::Decimal(1, 2).to_string(), "DECIMAL(1,2)");
    }
}
