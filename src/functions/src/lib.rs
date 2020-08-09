pub mod registry;
mod scalar;
use crate::registry::Registry;
use data::{DataType, Datum, Session};
use std::fmt::{Debug, Formatter};

/// The signature for a function. Signatures are scanned to find a match during planning.
/// The planner may up-cast values to make them fit if needed.
/// For decimal types etc the matching process will ignore the type parameters.
/// When using this to lookup a function the ret type is populated
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FunctionSignature<'a> {
    pub name: &'a str,
    pub args: Vec<DataType>,
    pub ret: DataType,
}

/// The definition of a function, enough info for resolving types etc
pub struct FunctionDefinition {
    pub signature: FunctionSignature<'static>,
    pub custom_return_type_resolver: Option<fn(&[DataType]) -> DataType>,
    pub function: &'static dyn Function,
}

impl Debug for FunctionDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("FunctionDefinition[")?;
        self.signature.fmt(f)?;
        f.write_str("]")
    }
}

impl FunctionDefinition {
    pub fn new(
        name: &'static str,
        args: Vec<DataType>,
        ret: DataType,
        function: &'static dyn Function,
    ) -> Self {
        FunctionDefinition {
            signature: FunctionSignature { name, args, ret },
            custom_return_type_resolver: None,
            function,
        }
    }

    pub fn new_with_type_resolver(
        name: &'static str,
        args: Vec<DataType>,
        return_type_resolver: fn(&[DataType]) -> DataType,
        function: &'static dyn Function,
    ) -> Self {
        let ret = return_type_resolver(&args);
        FunctionDefinition {
            signature: FunctionSignature { name, args, ret },
            custom_return_type_resolver: Some(return_type_resolver),
            function,
        }
    }
}

/// A function implementation
pub trait Function: Debug + Sync + 'static {
    fn execute<'a>(
        &self,
        session: &Session,
        signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a>;
}

fn register_builtins(registry: &mut Registry) {
    scalar::register_builtins(registry)
}
