use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::{DataType, Datum, Session};

/// Essentially a json -> string cast, but unlike the standard cast this wont quote contained strings
/// https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-unquote
/// If the contained json object is not a single top level string then the functionality is the same
/// as the standard json -> string cast.
#[derive(Debug)]
pub(super) struct JsonUnquote {}

impl Function for JsonUnquote {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(json) = args[0].as_maybe_json() {
            if let Some(s) = json.get_string() {
                Datum::from(s)
            } else {
                Datum::from(args[0].typed_with(DataType::Json).to_string())
            }
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "json_unquote",
        vec![DataType::Json],
        DataType::Text,
        FunctionType::Scalar(&JsonUnquote {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::json::OwnedJson;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "json_unquote",
        args: vec![],
        ret: DataType::Json,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            JsonUnquote {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_json_null() {
        // This is horrible behaviour but is working as designed.
        // https://bugs.mysql.com/bug.php?id=85755
        // It looks like the json value function fixes this problem
        let json = OwnedJson::parse(r#"null"#).unwrap();
        let expected_str = "null";

        assert_eq!(
            JsonUnquote {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(json)]),
            Datum::from(expected_str)
        )
    }

    #[test]
    fn test_number() {
        let json = OwnedJson::parse(r#"12345"#).unwrap();
        let expected_str = "12345";

        assert_eq!(
            JsonUnquote {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(json)]),
            Datum::from(expected_str)
        )
    }

    #[test]
    fn test_string() {
        let json = OwnedJson::parse(r#""hello world""#).unwrap();
        let expected_str = "hello world";

        assert_eq!(
            JsonUnquote {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(json)]),
            Datum::from(expected_str)
        )
    }

    #[test]
    fn test_complex() {
        let json = OwnedJson::parse(r#"["hello world",1,2]"#).unwrap();
        let expected_str = r#"["hello world",1,2]"#;

        assert_eq!(
            JsonUnquote {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(json)]),
            Datum::from(expected_str)
        )
    }
}
