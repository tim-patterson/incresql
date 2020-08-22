use crate::utils::logical::fields_for_operator;
use crate::{Field, Planner, PlannerError};
use ast::rel::logical::LogicalOperator;
use data::Session;

impl Planner {
    pub(crate) fn plan_common(
        &self,
        query: LogicalOperator,
        session: &Session,
    ) -> Result<(Vec<Field>, LogicalOperator), PlannerError> {
        let query = self.normalize(query)?;
        let query = self.validate(query, session)?;
        let query = self.optimize(query, session)?;
        let fields = fields_for_operator(&query).collect();
        Ok((fields, query))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{Expression, NamedExpression};
    use ast::rel::logical::Project;
    use data::rust_decimal::Decimal;
    use data::DataType;
    use std::str::FromStr;

    #[test]
    fn test_plan_common_fields() -> Result<(), PlannerError> {
        let planner = Planner::new_for_test();
        let session = Session::new(1);
        let raw_query = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::from(Decimal::from_str("1.23").unwrap()),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let (fields, _operator) = planner.plan_common(raw_query, &session)?;

        assert_eq!(
            fields,
            vec![Field {
                qualifier: None,
                alias: String::from("_col1"),
                data_type: DataType::Decimal(3, 2)
            }]
        );
        Ok(())
    }
}
