#[derive(Debug, Eq, PartialEq)]
pub enum DataType {
    // Could be considered a wildcard, ie nulls can be cast to anything
    Null,
    Boolean,
    Text,
    Integer,
    // Precision and scale
    Decimal(u8, u8),
}
