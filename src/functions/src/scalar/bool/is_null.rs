use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct IsNull {}

impl Function for IsNull {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        Datum::from(args[0].is_null())
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "isnull",
        vec![DataType::Null],
        DataType::Boolean,
        FunctionType::Scalar(&IsNull {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "isnull",
        args: vec![],
        ret: DataType::Boolean,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            IsNull {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null]),
            Datum::from(true)
        )
    }

    #[test]
    fn test_false() {
        assert_eq!(
            IsNull {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(false)]),
            Datum::from(false)
        );
    }

    #[test]
    fn test_one() {
        assert_eq!(
            IsNull {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(1)]),
            Datum::from(false)
        );
    }
}
