use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::json::JsonBuilder;
use data::{DataType, Datum, Session};

/// Extracts part of a json object using jsonpath, see
/// https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#function_json-extract
#[derive(Debug)]
pub(super) struct JsonExtract {}

impl Function for JsonExtract {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let (Some(json), Some(expr)) = (args[0].as_maybe_json(), args[1].as_maybe_jsonpath()) {
            if expr.could_return_many() {
                let json = JsonBuilder::default().array(|array| {
                    expr.evaluate(json, &mut (|json_match| array.push_json(json_match)))
                });
                Datum::from(json)
            } else {
                expr.evaluate_single(json)
                    .map(Datum::from)
                    .unwrap_or(Datum::Null)
            }
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "json_extract",
        vec![DataType::Json, DataType::JsonPath],
        DataType::Json,
        FunctionType::Scalar(&JsonExtract {}),
    ));
    registry.register_function(FunctionDefinition::new(
        "->",
        vec![DataType::Json, DataType::JsonPath],
        DataType::Json,
        FunctionType::Scalar(&JsonExtract {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::json::OwnedJson;
    use data::jsonpath_utils::JsonPathExpression;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "json_extract",
        args: vec![],
        ret: DataType::Json,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            JsonExtract {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::Null, Datum::from("foo")]
            ),
            Datum::Null
        )
    }

    #[test]
    fn test_single_path() {
        let json = OwnedJson::parse(r#"{"a": [1,2,3] }"#).unwrap();
        let json_path = Datum::Jsonpath(Box::new(JsonPathExpression::parse("$.a[1]").unwrap()));
        let expected_json = OwnedJson::parse(r#"2"#).unwrap();

        assert_eq!(
            JsonExtract {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(json), json_path]
            ),
            Datum::from(expected_json)
        )
    }

    #[test]
    fn test_wildcard_path() {
        let json = OwnedJson::parse(r#"{"a": [1,2], "b": [3,4] }"#).unwrap();
        let json_path = Datum::Jsonpath(Box::new(JsonPathExpression::parse("$.*[0]").unwrap()));
        let expected_json = OwnedJson::parse(r#"[1,3]"#).unwrap();

        assert_eq!(
            JsonExtract {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(json), json_path]
            ),
            Datum::from(expected_json)
        )
    }
}
