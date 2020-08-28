use crate::registry::Registry;
use crate::{CompoundFunction, CompoundFunctionArg, FunctionDefinition, FunctionType};
use data::DataType;

/// Combines the json_extract and json_unquote functions into a single
/// function, equiv to json_unquote(json_extract(<json>, <json_path>))
#[derive(Debug)]
struct JsonExtractUnquote {}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "->>",
        vec![DataType::Json, DataType::Text],
        DataType::Text,
        FunctionType::Compound(CompoundFunction {
            function_name: "json_unquote",
            args: vec![CompoundFunctionArg::Function(CompoundFunction {
                function_name: "json_extract",
                args: vec![CompoundFunctionArg::Input(0), CompoundFunctionArg::Input(1)],
            })],
        }),
    ));
}
