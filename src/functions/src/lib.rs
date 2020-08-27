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
    pub function: CompoundFunction,
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
        let signature = FunctionSignature { name, args, ret };
        FunctionDefinition {
            signature: signature.clone(),
            custom_return_type_resolver: None,
            function: CompoundFunction::from_single(function, signature),
        }
    }

    pub fn new_with_type_resolver(
        name: &'static str,
        args: Vec<DataType>,
        return_type_resolver: fn(&[DataType]) -> DataType,
        function: FunctionType,
    ) -> Self {
        let ret = return_type_resolver(&args);
        let signature = FunctionSignature { name, args, ret };
        FunctionDefinition {
            signature: signature.clone(),
            custom_return_type_resolver: Some(return_type_resolver),
            function: CompoundFunction::from_single(function, signature),
        }
    }
}

/// We want the ability to define functions in terms of other functions.
/// ->> should be defined as json_unquote(json_extract(x))
/// To support functions with more than one input we need to somehow represent
/// the composition as a tree with placeholders for the inputs.
/// The planner would then walk this structure to build up the actual expression
/// tree.
#[derive(Clone, Debug)]
pub struct CompoundFunction {
    pub signature: FunctionSignature<'static>,
    pub function: FunctionType,
    pub args: Vec<CompoundFunctionArg>,
}

#[derive(Clone, Debug)]
pub enum CompoundFunctionArg {
    Function(CompoundFunction),
    Input(usize),
}

impl CompoundFunction {
    pub(crate) fn from_single(
        function: FunctionType,
        signature: FunctionSignature<'static>,
    ) -> Self {
        let args = signature
            .args
            .iter()
            .enumerate()
            .map(|(idx, _)| CompoundFunctionArg::Input(idx))
            .collect();
        CompoundFunction {
            signature,
            function,
            args,
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
    /// Returns the size of the state (in number of datums) needed by this
    /// aggregate function
    fn state_size(&self) -> usize {
        1
    }

    /// Initializes the initial state
    fn initialize(&self, state: &mut [Datum<'static>]) {
        state[0] = Datum::Null;
    }

    /// Applies the new input to the state, for retractable
    /// aggregates the freq is simply negative
    fn apply(
        &self,
        signature: &FunctionSignature,
        args: &[Datum],
        freq: i64,
        state: &mut [Datum<'static>],
    );

    /// Merges two states together.
    fn merge(
        &self,
        signature: &FunctionSignature,
        input_state: &[Datum<'static>],
        state: &mut [Datum<'static>],
    );

    /// Render the final result from the state
    fn finalize<'a>(&self, _signature: &FunctionSignature, state: &'a [Datum<'a>]) -> Datum<'a> {
        state[0].ref_clone()
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
