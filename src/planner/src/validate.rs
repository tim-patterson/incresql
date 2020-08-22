use crate::common::{
    contains_aggregate, fieldnames_for_operator, fields_for_operator, source_fields_for_operator,
    type_for_expression,
};
use crate::{Field, FieldResolutionError, Planner, PlannerError};
use ast::expr::{
    ColumnReference, CompiledAggregate, CompiledColumnReference, CompiledFunctionCall, Expression,
    NamedExpression,
};
use ast::rel::logical::{GroupBy, LogicalOperator, ResolvedTable, TableInsert};
use data::{DataType, Datum, Session};
use functions::registry::Registry;
use functions::{FunctionSignature, FunctionType};

/// Validate the query, as part of the process of validating the query we will actually end up
/// doing all the catalog and function lookups and subbing them in.
impl Planner {
    pub fn validate(
        &self,
        mut query: LogicalOperator,
        session: &Session,
    ) -> Result<LogicalOperator, PlannerError> {
        self.resolve_tables(&mut query, session)?;
        validate_values_types(&mut query)?;
        expand_stars(&mut query)?;
        compile_functions(&mut query, &self.function_registry)?;
        project_to_groupby(&mut query);
        check_predicates(&mut query)?;
        check_union_alls(&mut query)?;
        check_inserts(&mut query)?;
        Ok(query)
    }

    fn resolve_tables(
        &self,
        operator: &mut LogicalOperator,
        session: &Session,
    ) -> Result<(), PlannerError> {
        for child in operator.children_mut() {
            self.resolve_tables(child, session)?;
        }

        if let LogicalOperator::TableReference(table_ref) = operator {
            let current_db = session.current_database.read().unwrap();
            let database = table_ref.database.as_ref().unwrap_or(&current_db);
            let table_name = &table_ref.table;
            let catalog = self.catalog.read().unwrap();

            let table = catalog.table(database, table_name)?;
            *operator = LogicalOperator::ResolvedTable(ResolvedTable { table })
        }

        Ok(())
    }
}

fn compile_functions(
    operator: &mut LogicalOperator,
    function_registry: &Registry,
) -> Result<(), PlannerError> {
    for child in operator.children_mut() {
        compile_functions(child, function_registry)?;
    }

    let source_fields: Vec<_> = source_fields_for_operator(operator).collect();
    for expr in operator.expressions_mut() {
        compile_functions_in_expr(expr, &source_fields, function_registry)?;
    }
    Ok(())
}

/// Walks the named expressions of projects looking for stars and replaces them with
/// column references from the sources.
fn expand_stars(operator: &mut LogicalOperator) -> Result<(), PlannerError> {
    /// Inner function to create a reference back to the field
    fn fields_to_ne(field: (Option<&str>, &str)) -> NamedExpression {
        NamedExpression {
            alias: Some(field.1.to_string()),
            expression: Expression::ColumnReference(ColumnReference {
                qualifier: field.0.map(str::to_string),
                alias: field.1.to_string(),
                star: false,
            }),
        }
    }

    for child in operator.children_mut() {
        expand_stars(child)?;
    }

    if let LogicalOperator::Project(project) = operator {
        let mut exprs = Vec::with_capacity(project.expressions.len());
        std::mem::swap(&mut exprs, &mut project.expressions);
        for ne in exprs {
            if let Expression::ColumnReference(ColumnReference {
                qualifier,
                alias: _,
                star: true,
            }) = ne.expression
            {
                if qualifier.is_some() {
                    project.expressions.extend(
                        fieldnames_for_operator(&project.source)
                            .filter(|(field_qualifier, _alias)| {
                                field_qualifier == &qualifier.as_deref()
                            })
                            .map(fields_to_ne),
                    );
                } else {
                    project
                        .expressions
                        .extend(fieldnames_for_operator(&project.source).map(fields_to_ne));
                }
            } else {
                project.expressions.push(ne);
            }
        }
    }
    Ok(())
}

fn compile_functions_in_expr(
    expression: &mut Expression,
    source_fields: &[Field],
    function_registry: &Registry,
) -> Result<(), PlannerError> {
    match expression {
        Expression::FunctionCall(function_call) => {
            for arg in function_call.args.iter_mut() {
                compile_functions_in_expr(arg, source_fields, function_registry)?;
            }

            let arg_types = function_call.args.iter().map(type_for_expression).collect();

            let lookup_sig = FunctionSignature {
                name: &function_call.function_name,
                args: arg_types,
                ret: DataType::Null,
            };

            let (signature, function) = function_registry.resolve_function(&lookup_sig)?;

            let mut args = Vec::new();
            std::mem::swap(&mut args, &mut function_call.args);

            *expression = match function {
                FunctionType::Scalar(function) => {
                    Expression::CompiledFunctionCall(CompiledFunctionCall {
                        function,
                        args: Box::from(args),
                        expr_buffer: Box::from(vec![]),
                        signature: Box::new(signature),
                    })
                }
                FunctionType::Aggregate(function) => {
                    Expression::CompiledAggregate(CompiledAggregate {
                        function,
                        args: Box::from(args),
                        expr_buffer: Box::from(vec![]),
                        signature: Box::new(signature),
                    })
                }
            };
        }
        Expression::Cast(cast) => {
            compile_functions_in_expr(&mut cast.expr, source_fields, function_registry)?;

            let expr_type = type_for_expression(&cast.expr);

            let function_name = match cast.datatype {
                DataType::Null => panic!("Attempted cast to null"),
                DataType::Boolean => "to_bool",
                DataType::Integer => "to_int",
                DataType::BigInt => "to_bigint",
                DataType::Decimal(..) => "to_decimal",
                DataType::Text => "to_text",
                DataType::ByteA => "to_bytes",
                DataType::Json => "to_json",
            };

            let lookup_sig = FunctionSignature {
                name: function_name,
                args: vec![expr_type],
                ret: cast.datatype,
            };

            let (signature, function) = function_registry.resolve_function(&lookup_sig)?;

            // Just an "empty" value to swap
            let mut expr = Expression::Constant(Datum::Null, DataType::Null);

            std::mem::swap(&mut expr, &mut cast.expr);
            if let FunctionType::Scalar(function) = function {
                *expression = Expression::CompiledFunctionCall(CompiledFunctionCall {
                    function,
                    args: Box::from(vec![expr]),
                    expr_buffer: Box::from(vec![]),
                    signature: Box::new(signature),
                })
            } else {
                panic!("Cast needs to be a scalar function")
            }
        }
        Expression::ColumnReference(column_reference) => {
            let indexed_source_fields = source_fields.iter().enumerate();
            let mut matching_fields: Vec<_> = if let Some(qualifier) = &column_reference.qualifier {
                indexed_source_fields
                    .filter(|(_idx, field)| {
                        field.qualifier.as_ref() == Some(qualifier)
                            && field.alias == column_reference.alias
                    })
                    .collect()
            } else {
                indexed_source_fields
                    .filter(|(_idx, field)| field.alias == column_reference.alias)
                    .collect()
            };

            if matching_fields.is_empty() {
                return Err(FieldResolutionError::NotFound(
                    ColumnReference::clone(column_reference),
                    source_fields.to_vec(),
                )
                .into());
            } else if matching_fields.len() > 1 {
                return Err(FieldResolutionError::Ambiguous(
                    ColumnReference::clone(column_reference),
                    matching_fields
                        .into_iter()
                        .map(|(_idx, field)| field.clone())
                        .collect(),
                )
                .into());
            } else {
                let (idx, field) = matching_fields.pop().unwrap();
                *expression = Expression::CompiledColumnReference(CompiledColumnReference {
                    offset: idx,
                    datatype: field.data_type,
                })
            }
        }

        // These are already good and for the ref/function call probably shouldn't exist yet.
        Expression::Constant(..)
        | Expression::CompiledFunctionCall(_)
        | Expression::CompiledAggregate(_)
        | Expression::CompiledColumnReference(_) => {}
    }
    Ok(())
}

/// Checks to make sure all predicate expressions are boolean expressions
fn check_predicates(operator: &mut LogicalOperator) -> Result<(), PlannerError> {
    for child in operator.children_mut() {
        check_predicates(child)?;
    }

    if let LogicalOperator::Filter(filter) = operator {
        match type_for_expression(&filter.predicate) {
            DataType::Boolean | DataType::Null => {}
            datatype => {
                return Err(PlannerError::PredicateNotBoolean(
                    datatype,
                    filter.predicate.clone(),
                ))
            }
        }
    }
    Ok(())
}

/// Checks to make sure the union all children are compatible with each other.
fn check_union_alls(operator: &mut LogicalOperator) -> Result<(), PlannerError> {
    for child in operator.children_mut() {
        check_union_alls(child)?;
    }

    if let LogicalOperator::UnionAll(union_all) = operator {
        let mut rest = union_all.sources.iter_mut().enumerate();
        let (_, first) = rest.next().unwrap();
        let first_fields: Vec<_> = fields_for_operator(first).map(|f| f.data_type).collect();
        for (operator_idx, operator) in rest {
            let fields: Vec<_> = fields_for_operator(operator).map(|f| f.data_type).collect();

            if first_fields != fields {
                return Err(PlannerError::UnionAllMismatch(
                    first_fields,
                    fields,
                    operator_idx,
                ));
            }
        }
    }

    Ok(())
}

/// Walks "values" (ie insert .. values ()) and populates types in the header,
/// has to happen fairly early on in the planning
fn validate_values_types(query: &mut LogicalOperator) -> Result<(), PlannerError> {
    for child in query.children_mut() {
        validate_values_types(child)?;
    }

    if let LogicalOperator::TableInsert(TableInsert { table, source }) = query {
        if let (LogicalOperator::Values(values), LogicalOperator::ResolvedTable(resolved_tables)) =
            (source.as_mut(), table.as_mut())
        {
            values.fields = resolved_tables
                .table
                .columns()
                .iter()
                .map(|(alias, dt)| (*dt, alias.clone()))
                .collect();

            let table_types: Vec<_> = values
                .fields
                .iter()
                .map(|(datatype, _)| *datatype)
                .collect();
            for row in &values.data {
                let row_types: Vec<_> = row.iter().map(type_for_expression).collect();
                let is_match = row_types
                    .iter()
                    .zip(table_types.iter())
                    .all(|(row, table)| row == table || *row == DataType::Null);
                if !is_match {
                    return Err(PlannerError::InsertMismatch(table_types, row_types));
                }
            }
        }
    }
    Ok(())
}

/// Checks to make sure we're inserting rows with the right datatypes/length
fn check_inserts(operator: &mut LogicalOperator) -> Result<(), PlannerError> {
    for child in operator.children_mut() {
        check_inserts(child)?;
    }

    if let LogicalOperator::TableInsert(table_insert) = operator {
        let table_fields: Vec<_> = fields_for_operator(&table_insert.table)
            .map(|f| f.data_type)
            .collect();
        let source_fields: Vec<_> = fields_for_operator(&table_insert.source)
            .map(|f| f.data_type)
            .collect();

        if table_fields != source_fields {
            Err(PlannerError::InsertMismatch(table_fields, source_fields))
        } else {
            Ok(())
        }
    } else {
        Ok(())
    }
}

/// Detects projects using aggregate functions and turns them into a group by.
fn project_to_groupby(operator: &mut LogicalOperator) {
    for child in operator.children_mut() {
        project_to_groupby(child);
    }
    if let LogicalOperator::Project(project) = operator {
        if project
            .expressions
            .iter()
            .any(|ne| contains_aggregate(&ne.expression))
        {
            let mut expressions = vec![];
            let mut source = Box::from(LogicalOperator::Single);
            std::mem::swap(&mut expressions, &mut project.expressions);
            std::mem::swap(&mut source, &mut project.source);

            *operator = LogicalOperator::GroupBy(GroupBy {
                expressions,
                key_expressions: vec![],
                source,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{FunctionCall, NamedExpression};
    use ast::rel::logical::{Project, TableReference};
    use data::{Datum, Session};
    use functions::Function;

    // A dummy function to use in the test cases.
    #[derive(Debug)]
    struct DummyFunct {}

    impl Function for DummyFunct {
        fn execute<'a>(
            &self,
            _session: &Session,
            _sig: &FunctionSignature,
            _args: &'a [Datum<'a>],
        ) -> Datum<'a> {
            unimplemented!()
        }
    }

    #[test]
    fn test_compile_function() -> Result<(), PlannerError> {
        let planner = Planner::new_for_test();
        let session = Session::new(1);
        let raw_query = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::FunctionCall(FunctionCall {
                    function_name: "+".to_string(),
                    args: vec![
                        Expression::from(1),
                        Expression::FunctionCall(FunctionCall {
                            function_name: "+".to_string(),
                            args: vec![Expression::from(2), Expression::from(3)],
                        }),
                    ],
                }),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let expected = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::CompiledFunctionCall(CompiledFunctionCall {
                    function: &DummyFunct {},
                    args: Box::from(vec![
                        Expression::from(1),
                        Expression::CompiledFunctionCall(CompiledFunctionCall {
                            function: &DummyFunct {},
                            args: Box::from(vec![Expression::from(2), Expression::from(3)]),
                            expr_buffer: Box::from(vec![]),
                            signature: Box::new(FunctionSignature {
                                name: "+",
                                args: vec![DataType::Integer, DataType::Integer],
                                ret: DataType::Integer,
                            }),
                        }),
                    ]),
                    expr_buffer: Box::from(vec![]),
                    signature: Box::new(FunctionSignature {
                        name: "+",
                        args: vec![DataType::Integer, DataType::Integer],
                        ret: DataType::Integer,
                    }),
                }),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let operator = planner.validate(raw_query, &session)?;

        assert_eq!(operator, expected);

        Ok(())
    }

    #[test]
    fn test_resolve_table_qualified() -> Result<(), PlannerError> {
        let planner = Planner::new_for_test();
        let session = Session::new(1);
        let raw_query = LogicalOperator::TableReference(TableReference {
            database: Some("incresql".to_string()),
            table: "databases".to_string(),
        });

        let operator = planner.validate(raw_query, &session)?;
        let fields: Vec<_> = fields_for_operator(&operator).collect();

        assert_eq!(
            fields,
            vec![Field {
                qualifier: None,
                alias: "name".to_string(),
                data_type: DataType::Text
            }]
        );

        Ok(())
    }

    #[test]
    fn test_resolve_table_unqualified() -> Result<(), PlannerError> {
        let planner = Planner::new_for_test();
        let session = Session::new(1);
        *session.current_database.write().unwrap() = "incresql".to_string();
        let raw_query = LogicalOperator::TableReference(TableReference {
            database: None,
            table: "databases".to_string(),
        });

        let operator = planner.validate(raw_query, &session)?;
        let fields: Vec<_> = fields_for_operator(&operator).collect();

        assert_eq!(
            fields,
            vec![Field {
                qualifier: None,
                alias: "name".to_string(),
                data_type: DataType::Text
            }]
        );

        Ok(())
    }
}
