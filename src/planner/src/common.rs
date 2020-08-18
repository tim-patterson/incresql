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
        let query = self.normalize(query)?;
        let query = self.validate(query, session)?;
        let query = self.optimize(query, session)?;
        let fields = fields_for_operator(&query).collect();
        Ok((fields, query))
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

/// Returns the fields for an operator, will panic if called before query is normalized
pub(crate) fn fields_for_operator(
    operator: &LogicalOperator,
) -> Box<dyn Iterator<Item = Field> + '_> {
    match operator {
        LogicalOperator::Project(project) => {
            Box::from(project.expressions.iter().map(|ne| Field {
                qualifier: None,
                alias: ne.alias.as_ref().unwrap().clone(),
                data_type: type_for_expression(&ne.expression),
            }))
        }
        LogicalOperator::Filter(filter) => fields_for_operator(&filter.source),
        LogicalOperator::Limit(limit) => fields_for_operator(&limit.source),
        LogicalOperator::Values(values) => {
            Box::from(values.fields.iter().map(|(data_type, alias)| Field {
                qualifier: None,
                alias: alias.clone(),
                data_type: *data_type,
            }))
        }
        LogicalOperator::TableAlias(table_alias) => Box::from(
            fields_for_operator(&table_alias.source).map(move |f| Field {
                qualifier: Some(table_alias.alias.clone()),
                ..f
            }),
        ),
        LogicalOperator::UnionAll(union_all) => {
            fields_for_operator(union_all.sources.first().unwrap())
        }
        LogicalOperator::Single => Box::from(empty()),
        LogicalOperator::ResolvedTable(table) => {
            Box::from(table.table.columns().iter().map(|(alias, datatype)| Field {
                qualifier: None,
                alias: alias.clone(),
                data_type: *datatype,
            }))
        }
        LogicalOperator::TableReference(_) => panic!(),
    }
}

/// A much lighter version of fields_for_operator that can be run before function
/// and type resolution
pub(crate) fn fieldnames_for_operator(
    operator: &LogicalOperator,
) -> Box<dyn Iterator<Item = (Option<&str>, &str)> + '_> {
    match operator {
        LogicalOperator::Project(project) => Box::from(
            project
                .expressions
                .iter()
                .map(|ne| (None, ne.alias.as_ref().unwrap().as_str())),
        ),
        LogicalOperator::Filter(filter) => fieldnames_for_operator(&filter.source),
        LogicalOperator::Limit(limit) => fieldnames_for_operator(&limit.source),
        LogicalOperator::Values(values) => Box::from(
            values
                .fields
                .iter()
                .map(|(_datatype, alias)| (None, alias.as_str())),
        ),
        LogicalOperator::TableAlias(table_alias) => Box::from(
            fieldnames_for_operator(&table_alias.source)
                .map(move |(_, alias)| (Some(table_alias.alias.as_str()), alias)),
        ),
        LogicalOperator::UnionAll(union_all) => {
            fieldnames_for_operator(union_all.sources.first().unwrap())
        }
        LogicalOperator::Single => Box::from(empty()),
        LogicalOperator::ResolvedTable(table) => Box::from(
            table
                .table
                .columns()
                .iter()
                .map(|(alias, _datatype)| (None, alias.as_str())),
        ),
        LogicalOperator::TableReference(_) => panic!(),
    }
}

/// Returns the source fields for an operator.
/// This is the fields that expressions in the operator can "see".
/// For now this is only going to be expressions from the immediate children.
pub(crate) fn source_fields_for_operator(
    operator: &LogicalOperator,
) -> Box<dyn Iterator<Item = Field> + '_> {
    match operator {
        LogicalOperator::Project(project) => fields_for_operator(&project.source),
        LogicalOperator::Filter(filter) => fields_for_operator(&filter.source),
        LogicalOperator::Limit(limit) => fields_for_operator(&limit.source),
        LogicalOperator::TableAlias(table_alias) => fields_for_operator(&table_alias.source),
        LogicalOperator::UnionAll(union_all) => {
            fields_for_operator(union_all.sources.first().unwrap())
        }
        LogicalOperator::Values(_)
        | LogicalOperator::Single
        | LogicalOperator::TableReference(_)
        | LogicalOperator::ResolvedTable(_) => Box::from(empty()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::NamedExpression;
    use ast::rel::logical::{Project, TableAlias};
    use data::rust_decimal::Decimal;
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

    #[test]
    fn test_fields_for_operator() {
        let projection = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: Some("bar".to_string()),
                expression: Expression::from(Decimal::from_str("1.23").unwrap()),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        assert_eq!(
            fields_for_operator(&projection).collect::<Vec<_>>(),
            vec![Field {
                qualifier: None,
                alias: "bar".to_string(),
                data_type: DataType::Decimal(3, 2)
            }]
        );

        let table_alias = LogicalOperator::TableAlias(TableAlias {
            alias: "foo".to_string(),
            source: Box::new(projection),
        });

        assert_eq!(
            fields_for_operator(&table_alias).collect::<Vec<_>>(),
            vec![Field {
                qualifier: Some("foo".to_string()),
                alias: "bar".to_string(),
                data_type: DataType::Decimal(3, 2)
            }]
        );
    }

    #[test]
    fn test_fieldnames_for_operator() {
        let projection = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: Some("bar".to_string()),
                expression: Expression::from(Decimal::from_str("1.23").unwrap()),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        assert_eq!(
            fieldnames_for_operator(&projection).collect::<Vec<_>>(),
            vec![(None, "bar")]
        );
    }
}
