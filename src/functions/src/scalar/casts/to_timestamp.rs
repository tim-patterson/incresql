use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::chrono::NaiveDateTime;
use data::{DataType, Datum, Session};
use std::str::FromStr;

#[derive(Debug)]
struct ToTimestampFromText {}

impl Function for ToTimestampFromText {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(s) = args[0].as_maybe_text() {
            NaiveDateTime::from_str(s)
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
        "to_timestamp",
        vec![DataType::Text],
        DataType::Timestamp,
        FunctionType::Scalar(&ToTimestampFromText {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::chrono::{NaiveDate, NaiveTime};

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "to_date",
        args: vec![],
        ret: DataType::Date,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            ToTimestampFromText {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_from_text() {
        assert_eq!(
            ToTimestampFromText {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from("2010-10-01T10:00:00")]
            ),
            Datum::from(NaiveDateTime::new(
                NaiveDate::from_ymd(2010, 10, 1),
                NaiveTime::from_hms(10, 0, 0)
            ))
        )
    }

    #[test]
    fn test_from_text_malformed() {
        assert_eq!(
            ToTimestampFromText {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from("2010-1s0-01")]
            ),
            Datum::Null
        )
    }
}
