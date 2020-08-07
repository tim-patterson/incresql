use crate::normalize::normalize;
use crate::validate::validate;
use crate::{Field, PlannerError};
use ast::expr::Expression;
use ast::rel::logical::LogicalOperator;
use data::DataType;

pub(crate) fn plan_common(
    query: LogicalOperator,
) -> Result<(Vec<Field>, LogicalOperator), PlannerError> {
    let query = validate(query)?;
    let query = normalize(query)?;
    let fields = fields_for_operator(&query).collect();
    Ok((fields, query))
}

/// Returns the fields for an operator, will panic if called before query is normalized
fn fields_for_operator(operator: &LogicalOperator) -> impl Iterator<Item = Field> + '_ {
    operator.named_expressions().map(|ne| Field {
        alias: ne.alias.as_ref().unwrap().clone(),
        data_type: type_for_expression(&ne.expression),
    })
}

/// Returns the datatype for an expression, will panic if called before query is normalized
fn type_for_expression(expr: &Expression) -> DataType {
    match expr {
        Expression::Literal(constant) => constant.datatype(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::NamedExpression;
    use ast::rel::logical::Project;
    use data::{Datum, Decimal};
    use std::str::FromStr;

    #[test]
    fn test_plan_common_fields() -> Result<(), PlannerError> {
        let raw_query = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::Literal(Datum::from(Decimal::from_str("1.23").unwrap())),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let (fields, _operator) = plan_common(raw_query)?;

        assert_eq!(
            fields,
            vec![Field {
                alias: String::from("_col1"),
                data_type: DataType::Decimal(28, 2)
            }]
        );
        Ok(())
    }
}
