use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::chrono::Duration;
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct DateSub {}

/// date_sub(date, int)
impl Function for DateSub {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let (Some(date), Some(days)) = (args[0].as_maybe_date(), args[1].as_maybe_integer()) {
            Datum::from(date - Duration::days(days as i64))
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "date_sub",
        vec![DataType::Date, DataType::Integer],
        DataType::Date,
        FunctionType::Scalar(&DateSub {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::chrono::NaiveDate;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "date_sub",
        args: vec![],
        ret: DataType::Date,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            DateSub {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null, Datum::from(5)]),
            Datum::Null
        )
    }

    #[test]
    fn test_date_sub() {
        assert_eq!(
            DateSub {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[
                    Datum::from(NaiveDate::from_ymd(2020, 05, 15)),
                    Datum::from(5)
                ]
            ),
            Datum::from(NaiveDate::from_ymd(2020, 05, 10))
        )
    }
}
