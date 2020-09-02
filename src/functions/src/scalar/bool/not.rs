use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct Not {}

impl Function for Not {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(b) = args[0].as_maybe_boolean() {
            Datum::from(!b)
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "not",
        vec![DataType::Boolean],
        DataType::Boolean,
        FunctionType::Scalar(&Not {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "not",
        args: vec![],
        ret: DataType::Boolean,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            Not {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_true() {
        assert_eq!(
            Not {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(true)]),
            Datum::from(false)
        );
    }

    #[test]
    fn test_false() {
        assert_eq!(
            Not {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(false)]),
            Datum::from(true)
        );
    }
}
