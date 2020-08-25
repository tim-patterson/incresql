use crate::jsonpath_utils::JsonPathExpression;
use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::json::JsonBuilder;
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct JsonExtract {}

impl Function for JsonExtract {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let (Some(json), Some(json_path)) = (args[0].as_maybe_json(), args[1].as_maybe_text()) {
            if let Some(expr) = JsonPathExpression::parse(json_path) {
                if expr.could_return_many() {
                    let json = JsonBuilder::default().array(|array| {
                        for json_match in expr.evaluate(json) {
                            array.push_json(json_match);
                        }
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
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "json_extract",
        vec![DataType::Json, DataType::Text],
        DataType::Json,
        FunctionType::Scalar(&JsonExtract {}),
    ));
    registry.register_function(FunctionDefinition::new(
        "->",
        vec![DataType::Json, DataType::Text],
        DataType::Json,
        FunctionType::Scalar(&JsonExtract {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::json::OwnedJson;

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
    fn test_bad_path() {
        let json = OwnedJson::parse("{}").unwrap();

        assert_eq!(
            JsonExtract {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(json), Datum::from("foo")]
            ),
            Datum::Null
        )
    }

    #[test]
    fn test_single_path() {
        let json = OwnedJson::parse(r#"{"a": [1,2,3] }"#).unwrap();
        let json_path = "$.a[1]";
        let expected_json = OwnedJson::parse(r#"2"#).unwrap();

        assert_eq!(
            JsonExtract {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(json), Datum::from(json_path)]
            ),
            Datum::from(expected_json)
        )
    }

    #[test]
    fn test_wildcard_path() {
        let json = OwnedJson::parse(r#"{"a": [1,2], "b": [3,4] }"#).unwrap();
        let json_path = "$.*[0]";
        let expected_json = OwnedJson::parse(r#"[1,3]"#).unwrap();

        assert_eq!(
            JsonExtract {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(json), Datum::from(json_path)]
            ),
            Datum::from(expected_json)
        )
    }
}
