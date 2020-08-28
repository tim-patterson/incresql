use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::jsonpath_utils::JsonPathExpression;
use data::{DataType, Datum, Session};

/// Compiles a jsonpath expression into a json object
#[derive(Debug)]
struct ToJsonpath {}

impl Function for ToJsonpath {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(json_path) = args[0].as_maybe_text() {
            if let Some(expr) = JsonPathExpression::parse(json_path) {
                Datum::Jsonpath(Box::new(expr))
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
        "to_jsonpath",
        vec![DataType::Text],
        DataType::JsonPath,
        FunctionType::Scalar(&ToJsonpath {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "to_jsonpath",
        args: vec![],
        ret: DataType::JsonPath,
    };

    #[test]
    fn test_text() {
        // Casts from text actually parses the jsonpath
        assert_eq!(
            ToJsonpath {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(r#"$.foo"#)]),
            Datum::Jsonpath(Box::from(JsonPathExpression::parse("$.foo").unwrap()))
        );
    }
}
