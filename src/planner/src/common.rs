use crate::{Field, Planner, PlannerError};
use ast::expr::Expression;
use ast::rel::logical::LogicalOperator;
use data::{DataType, Session};
use std::iter::empty;

impl Planner {
    pub(crate) fn plan_common(
        &self,
        query: LogicalOperator,
        session: &Session,
    ) -> Result<(Vec<Field>, LogicalOperator), PlannerError> {
        let query = self.validate(query)?;
        let query = self.normalize(query)?;
        let query = self.optimize(query, session)?;
        let fields = fields_for_operator(&query).collect();
        Ok((fields, query))
    }
}

/// Returns the fields for an operator, will panic if called before query is normalized
fn fields_for_operator(operator: &LogicalOperator) -> Box<dyn Iterator<Item = Field> + '_> {
    match operator {
        LogicalOperator::Project(_) => Box::from(operator.named_expressions().map(|ne| Field {
            alias: ne.alias.as_ref().unwrap().clone(),
            data_type: type_for_expression(&ne.expression),
        })),
        LogicalOperator::Values(values) => {
            Box::from(values.fields.iter().map(|(data_type, alias)| Field {
                alias: alias.clone(),
                data_type: *data_type,
            }))
        }
        LogicalOperator::TableAlias(table_alias) => fields_for_operator(&table_alias.source),
        LogicalOperator::Single => Box::from(empty()),
    }
}

/// Returns the datatype for an expression, will panic if called before query is normalized
pub(crate) fn type_for_expression(expr: &Expression) -> DataType {
    match expr {
        Expression::Constant(_constant, datatype) => *datatype,
        Expression::Cast(cast) => cast.datatype,
        Expression::CompiledFunctionCall(function_call) => function_call.signature.ret,
        Expression::CompiledColumnReference(column_reference) => column_reference.datatype,

        // These should be gone by now!
        Expression::FunctionCall(_) | Expression::ColumnReference(_) => {
            panic!("These should be gone by now!")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::NamedExpression;
    use ast::rel::logical::Project;
    use data::rust_decimal::Decimal;
    use functions::registry::Registry;
    use std::str::FromStr;

    #[test]
    fn test_plan_common_fields() -> Result<(), PlannerError> {
        let planner = Planner::new(Registry::new(false));
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
                alias: String::from("_col1"),
                data_type: DataType::Decimal(3, 2)
            }]
        );
        Ok(())
    }
}
