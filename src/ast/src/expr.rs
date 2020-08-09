use data::{DataType, Datum};
use functions::{Function, FunctionSignature};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Expression {
    Literal(Datum<'static>),
    FunctionCall(FunctionCall),
    Cast(Cast),
    CompiledFunctionCall(CompiledFunctionCall),
}

/// Represents a function call straight from the parser.
/// Ie the function isn't actually resolved by this point
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FunctionCall {
    pub function_name: String,
    pub args: Vec<Expression>,
}

/// Represents a sql cast, gets compiled to a function
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Cast {
    pub expr: Box<Expression>,
    pub datatype: DataType,
}

/// Represents a function call once its been resolved and type
/// checked
#[derive(Debug, Clone)]
pub struct CompiledFunctionCall {
    pub function: &'static dyn Function,
    pub args: Vec<Expression>,
    // Used to store the evaluation results of the sub expressions
    pub expr_buffer: Vec<Datum<'static>>,
    // Boxed to keep size of expression down
    pub signature: Box<FunctionSignature<'static>>,
}

impl PartialEq for CompiledFunctionCall {
    fn eq(&self, other: &Self) -> bool {
        self.args == other.args && self.signature == other.signature
    }
}

impl Eq for CompiledFunctionCall {}

/// Named expression, ie select foo as bar
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NamedExpression {
    pub alias: Option<String>,
    pub expression: Expression,
}
