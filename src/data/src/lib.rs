// Re-exported as almost every crate using data will also need decimal
pub use rust_decimal;
mod datatype;
mod datum;
pub mod encoding;
mod session;
pub use datatype::*;
pub use datum::Datum;
pub use session::Session;

/// General sort order enum.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    pub fn is_asc(&self) -> bool {
        *self == SortOrder::Asc
    }

    pub fn is_desc(&self) -> bool {
        *self == SortOrder::Desc
    }
}
