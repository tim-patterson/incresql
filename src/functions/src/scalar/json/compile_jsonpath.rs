use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::jsonpath_utils::JsonPathExpression;
use data::{DataType, Datum, Session};

/// Compiles a jsonpath expression into a json object
#[derive(Debug)]
pub(super) struct CompileJsonpath {}

impl Function for CompileJsonpath {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(json_path) = args[0].as_maybe_text() {
            if let Some(expr) = JsonPathExpression::parse(json_path) {
                Datum::CompiledJsonpath(Box::new(expr))
            } else {
                Datum::Null
            }
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "$$compile_jsonpath",
        vec![DataType::Text],
        DataType::CompiledJsonPath,
        FunctionType::Scalar(&CompileJsonpath {}),
    ));
}
