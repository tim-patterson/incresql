use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::json::{JsonBuilder, OwnedJson};
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct ToJsonFromBoolean {}

impl Function for ToJsonFromBoolean {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(b) = args[0].as_maybe_boolean() {
            Datum::from(JsonBuilder::default().bool(b))
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToJsonFromInt {}

impl Function for ToJsonFromInt {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(i) = args[0].as_maybe_integer() {
            Datum::from(JsonBuilder::default().int(i as i64))
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToJsonFromBigInt {}

impl Function for ToJsonFromBigInt {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(i) = args[0].as_maybe_bigint() {
            Datum::from(JsonBuilder::default().int(i))
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToJsonFromDecimal {}

impl Function for ToJsonFromDecimal {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(d) = args[0].as_maybe_decimal() {
            Datum::from(JsonBuilder::default().decimal(d))
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToJsonFromText {}

impl Function for ToJsonFromText {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(json) = args[0].as_maybe_text().and_then(OwnedJson::parse) {
            Datum::from(json)
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "to_json",
        vec![DataType::Boolean],
        DataType::Json,
        FunctionType::Scalar(&ToJsonFromBoolean {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "to_json",
        vec![DataType::Integer],
        DataType::Json,
        FunctionType::Scalar(&ToJsonFromInt {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "to_json",
        vec![DataType::BigInt],
        DataType::Json,
        FunctionType::Scalar(&ToJsonFromBigInt {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "to_json",
        vec![DataType::Decimal(0, 0)],
        DataType::Json,
        FunctionType::Scalar(&ToJsonFromDecimal {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "to_json",
        vec![DataType::Text],
        DataType::Json,
        FunctionType::Scalar(&ToJsonFromText {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::rust_decimal::Decimal;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "to_json",
        args: vec![],
        ret: DataType::Json,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            ToJsonFromBoolean {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_boolean() {
        assert_eq!(
            ToJsonFromBoolean {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(true)]),
            Datum::from(JsonBuilder::default().bool(true))
        )
    }

    #[test]
    fn test_int() {
        assert_eq!(
            ToJsonFromInt {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(1)]),
            Datum::from(JsonBuilder::default().int(1))
        )
    }

    #[test]
    fn test_bigint() {
        assert_eq!(
            ToJsonFromBigInt {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(1 as i64)]),
            Datum::from(JsonBuilder::default().int(1))
        )
    }

    #[test]
    fn test_decimal() {
        assert_eq!(
            ToJsonFromDecimal {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(Decimal::new(1234, 2))]
            ),
            Datum::from(JsonBuilder::default().decimal(Decimal::new(1234, 2)))
        )
    }

    #[test]
    fn test_text() {
        // Casts from text actually parse the json.
        assert_eq!(
            ToJsonFromText {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(r#"{"k": [1,2]}"#)]
            ),
            Datum::from(JsonBuilder::default().object(|object| {
                object.push_array("k", |array| {
                    array.push_int(1);
                    array.push_int(2);
                })
            }))
        );
    }
}
