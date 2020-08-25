use crate::registry::Registry;
use crate::scalar::json::json_extract::JsonExtract;
use crate::scalar::json::json_unquote::JsonUnquote;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::{DataType, Datum, Session};

/// Combines the json_extract and json_unquote functions into a single
/// function, equiv to json_unquote(json_extract(<json>, <json_path>))
#[derive(Debug)]
struct JsonExtractUnquote {}

impl Function for JsonExtractUnquote {
    fn execute<'a>(
        &self,
        session: &Session,
        signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        let unquote_args = [JsonExtract {}.execute(session, signature, args)];
        // We need the as static here as otherwise it would be left tied to our temp datum above
        JsonUnquote {}
            .execute(session, signature, unquote_args.as_ref())
            .into_static()
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "->>",
        vec![DataType::Json, DataType::Text],
        DataType::Text,
        FunctionType::Scalar(&JsonExtractUnquote {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::json::OwnedJson;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "->>",
        args: vec![],
        ret: DataType::Text,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            JsonExtractUnquote {}.execute(
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
            JsonExtractUnquote {}.execute(
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
        let expected_text = "2";

        assert_eq!(
            JsonExtractUnquote {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(json), Datum::from(json_path)]
            ),
            Datum::from(expected_text)
        )
    }

    #[test]
    fn test_single_path_to_string() {
        let json = OwnedJson::parse(r#"{"a": [1,"abc",3] }"#).unwrap();
        let json_path = "$.a[1]";
        let expected_text = "abc";

        assert_eq!(
            JsonExtractUnquote {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(json), Datum::from(json_path)]
            ),
            Datum::from(expected_text)
        )
    }

    #[test]
    fn test_wildcard_path() {
        let json = OwnedJson::parse(r#"{"a": [1,2], "b": [3,4] }"#).unwrap();
        let json_path = "$.*[0]";
        let expected_text = "[1,3]";

        assert_eq!(
            JsonExtractUnquote {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(json), Datum::from(json_path)]
            ),
            Datum::from(expected_text)
        )
    }
}
