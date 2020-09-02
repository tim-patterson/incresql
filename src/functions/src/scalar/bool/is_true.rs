use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct IsTrue {}

impl Function for IsTrue {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        Datum::from(args[0] == Datum::from(true))
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "istrue",
        vec![DataType::Boolean],
        DataType::Boolean,
        FunctionType::Scalar(&IsTrue {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "istrue",
        args: vec![],
        ret: DataType::Boolean,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            IsTrue {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null]),
            Datum::from(false)
        )
    }

    #[test]
    fn test_false() {
        assert_eq!(
            IsTrue {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(false)]),
            Datum::from(false)
        );
    }

    #[test]
    fn test_true() {
        assert_eq!(
            IsTrue {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(true)]),
            Datum::from(true)
        );
    }
}
