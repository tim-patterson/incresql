use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct IsFalse {}

impl Function for IsFalse {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        Datum::from(args[0] == Datum::from(false))
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "isfalse",
        vec![DataType::Boolean],
        DataType::Boolean,
        FunctionType::Scalar(&IsFalse {}),
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
            IsFalse {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null]),
            Datum::from(false)
        )
    }

    #[test]
    fn test_false() {
        assert_eq!(
            IsFalse {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(false)]),
            Datum::from(true)
        );
    }

    #[test]
    fn test_true() {
        assert_eq!(
            IsFalse {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(true)]),
            Datum::from(false)
        );
    }
}
