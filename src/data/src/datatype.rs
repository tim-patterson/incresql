#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum DataType {
    // Could be considered a wildcard, ie nulls can be cast to anything
    Null,
    Boolean,
    Text,
    Integer,
    BigInt,
    // Precision and scale
    Decimal(u8, u8),
}

pub const DECIMAL_MAX_PRECISION: u8 = 28;
pub const DECIMAL_MAX_SCALE: u8 = 14;
