#[derive(Debug)]
pub enum DataType {
    Boolean,
    Text,
    Integer,
    // Precision and scale
    Decimal(u8, u8),
}
