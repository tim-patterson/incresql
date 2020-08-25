mod aggregate;
mod jsonpath_utils;
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
    pub function: FunctionType,
}

#[derive(Copy, Clone, Debug)]
pub enum FunctionType {
    Scalar(&'static dyn Function),
    Aggregate(&'static dyn AggregateFunction),
}

impl FunctionType {
    /// Helper for tests, unwraps the scalar function inside
    pub fn as_scalar(&self) -> &'static dyn Function {
        if let FunctionType::Scalar(f) = self {
            *f
        } else {
            panic!()
        }
    }
    /// Helper for tests, unwraps the aggregate function inside
    pub fn as_aggregate(&self) -> &'static dyn AggregateFunction {
        if let FunctionType::Aggregate(f) = self {
            *f
        } else {
            panic!()
        }
    }
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
        function: FunctionType,
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
        function: FunctionType,
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

/// A function implementation for aggregate functions.
pub trait AggregateFunction: Debug + Sync + 'static {
    /// Returns a new "empty"/initial state
    fn initialize(&self) -> Datum<'static> {
        Datum::Null
    }

    /// Applies the new input to the state, for retractable
    /// aggregates the freq is simply negative
    fn apply(
        &self,
        signature: &FunctionSignature,
        args: &[Datum],
        freq: i64,
        state: &mut Datum<'static>,
    );

    /// Merges two states together.
    fn merge(
        &self,
        signature: &FunctionSignature,
        input_state: &Datum<'static>,
        state: &mut Datum<'static>,
    );

    /// Render the final result from the state
    fn finalize<'a>(&self, _signature: &FunctionSignature, state: &'a Datum<'a>) -> Datum<'a> {
        state.ref_clone()
    }

    /// Can we undo records from this aggregate. Postgres calls these
    /// moving-aggregates, as well as supporting streaming group bys
    /// with allot less state it also makes window functions fast and
    /// efficient
    fn supports_retract(&self) -> bool {
        false
    }
}

fn register_builtins(registry: &mut Registry) {
    aggregate::register_builtins(registry);
    scalar::register_builtins(registry);
}
