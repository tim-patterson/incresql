use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct Or {}

impl Function for Or {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let (Some(a), Some(b)) = (args[0].as_maybe_boolean(), args[1].as_maybe_boolean()) {
            Datum::from(a || b)
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "or",
        vec![DataType::Boolean, DataType::Boolean],
        DataType::Boolean,
        FunctionType::Scalar(&Or {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "or",
        args: vec![],
        ret: DataType::Boolean,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            Or {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::Null, Datum::from(true)]
            ),
            Datum::Null
        )
    }

    #[test]
    fn test_true() {
        assert_eq!(
            Or {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(true), Datum::from(false)]
            ),
            Datum::from(true)
        );
    }

    #[test]
    fn test_false() {
        assert_eq!(
            Or {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(false), Datum::from(false)]
            ),
            Datum::from(false)
        );
    }
}
