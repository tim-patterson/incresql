use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature};
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct ToBooleanFromBoolean {}

impl Function for ToBooleanFromBoolean {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        args[0].clone()
    }
}

#[derive(Debug)]
struct ToBooleanFromText {}

impl Function for ToBooleanFromText {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(a) = args[0].as_text() {
            if a.eq_ignore_ascii_case("true") {
                Datum::from(true)
            } else if a.eq_ignore_ascii_case("false") {
                Datum::from(false)
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
        "to_bool",
        vec![DataType::Boolean],
        DataType::Boolean,
        &ToBooleanFromBoolean {},
    ));

    registry.register_function(FunctionDefinition::new(
        "to_bool",
        vec![DataType::Text],
        DataType::Boolean,
        &ToBooleanFromText {},
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "to_bool",
        args: vec![],
        ret: DataType::Boolean,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            ToBooleanFromBoolean {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_from_text() {
        assert_eq!(
            ToBooleanFromText {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from("True")]),
            Datum::from(true)
        )
    }
}
