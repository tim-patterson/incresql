mod scalar;
use data::{DataType, Datum, Session};
use std::fmt::Debug;

/// The signature for a function. Signatures are scanned to find a match during planning.
/// The planner may up-cast values to make them fit if needed.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FunctionSignature {
    pub name: &'static str,
    pub args: Vec<DataType>,
    pub ret: DataType,
}

/// A function implementation
pub trait Function: Debug {
    fn execute<'a>(&self, session: &Session, args: &'a [Datum<'a>]) -> Datum<'a>;
}
