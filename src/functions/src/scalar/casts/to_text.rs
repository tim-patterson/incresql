use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature};
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct ToTextFromText {}

impl Function for ToTextFromText {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        args[0].ref_clone()
    }
}

#[derive(Debug)]
struct ToTextFromBoolean {}

impl Function for ToTextFromBoolean {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(b) = args[0].as_boolean() {
            if b {
                Datum::from("TRUE")
            } else {
                Datum::from("FALSE")
            }
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToTextFromAny {}

impl Function for ToTextFromAny {
    fn execute<'a>(
        &self,
        _session: &Session,
        signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if args[0] == Datum::Null {
            Datum::Null
        } else {
            Datum::from(args[0].typed_with(signature.args[0]).to_string())
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "to_text",
        vec![DataType::Boolean],
        DataType::Text,
        &ToTextFromBoolean {},
    ));

    registry.register_function(FunctionDefinition::new(
        "to_text",
        vec![DataType::Text],
        DataType::Text,
        &ToTextFromText {},
    ));

    registry.register_function(FunctionDefinition::new(
        "to_text",
        vec![DataType::Null],
        DataType::Text,
        &ToTextFromAny {},
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::rust_decimal::Decimal;

    fn sig(input_type: DataType) -> FunctionSignature<'static> {
        FunctionSignature {
            name: "to_text",
            args: vec![input_type],
            ret: DataType::Text,
        }
    }

    #[test]
    fn test_null() {
        assert_eq!(
            ToTextFromText {}.execute(&Session::new(1), &sig(DataType::Text), &[Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_from_bool() {
        assert_eq!(
            ToTextFromBoolean {}.execute(
                &Session::new(1),
                &sig(DataType::Boolean),
                &[Datum::from(true)]
            ),
            Datum::ByteARef(b"TRUE")
        )
    }

    #[test]
    fn test_from_int() {
        assert_eq!(
            ToTextFromAny {}.execute(&Session::new(1), &sig(DataType::Integer), &[Datum::from(1)]),
            Datum::from("1".to_string())
        )
    }

    #[test]
    fn test_from_bigint() {
        assert_eq!(
            ToTextFromAny {}.execute(
                &Session::new(1),
                &sig(DataType::BigInt),
                &[Datum::from(1_i64)]
            ),
            Datum::from("1".to_string())
        )
    }

    #[test]
    fn test_from_decimal() {
        assert_eq!(
            ToTextFromAny {}.execute(
                &Session::new(1),
                &sig(DataType::Decimal(10, 1)),
                &[Datum::from(Decimal::new(10, 1))]
            ),
            Datum::from("1.0".to_string())
        );

        assert_eq!(
            ToTextFromAny {}.execute(
                &Session::new(1),
                &sig(DataType::Decimal(10, 2)),
                &[Datum::from(Decimal::new(10, 1))]
            ),
            Datum::from("1.00".to_string())
        )
    }

    #[test]
    fn test_from_text() {
        // String Ref
        assert_eq!(
            ToTextFromText {}.execute(&Session::new(1), &sig(DataType::Text), &[Datum::from("1")]),
            Datum::from("1")
        );

        // String Owned
        assert_eq!(
            ToTextFromText {}.execute(
                &Session::new(1),
                &sig(DataType::Text),
                &[Datum::from("1".to_string())]
            ),
            Datum::from("1")
        )
    }
}
