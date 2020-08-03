// Re-exported as almost every crate using data will also need decimal
pub use rust_decimal::Decimal;
mod datatype;
mod datum;
pub use datatype::DataType;
pub use datum::Datum;
