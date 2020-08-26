use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::chrono::NaiveDate;
use data::{DataType, Datum, Session};
use std::str::FromStr;

#[derive(Debug)]
struct ToDateFromText {}

impl Function for ToDateFromText {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(s) = args[0].as_maybe_text() {
            NaiveDate::from_str(s)
                .ok()
                .map(Datum::from)
                .unwrap_or_default()
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "to_date",
        vec![DataType::Text],
        DataType::Date,
        FunctionType::Scalar(&ToDateFromText {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "to_date",
        args: vec![],
        ret: DataType::Date,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            ToDateFromText {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_from_text() {
        assert_eq!(
            ToDateFromText {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from("2010-10-01")]),
            Datum::from(NaiveDate::from_ymd(2010, 10, 1))
        )
    }

    #[test]
    fn test_from_text_malformed() {
        assert_eq!(
            ToDateFromText {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from("2010-1s0-01")]),
            Datum::Null
        )
    }
}
