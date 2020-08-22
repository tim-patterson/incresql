use crate::utils::logical::fieldnames_for_operator;
use ast::expr::*;
use ast::rel::logical::LogicalOperator;

/// Walks the named expressions of projects looking for stars and replaces them with
/// column references from the sources.
pub(super) fn expand_stars(operator: &mut LogicalOperator) {
    for child in operator.children_mut() {
        expand_stars(child);
    }

    let (source_expressions, source) = match operator {
        LogicalOperator::Project(project) => (&mut project.expressions, &project.source),
        LogicalOperator::GroupBy(group_by) => (&mut group_by.expressions, &group_by.source),
        _ => return,
    };

    let mut expressions = Vec::with_capacity(source_expressions.len());
    std::mem::swap(&mut expressions, source_expressions);

    for ne in expressions {
        if let Expression::ColumnReference(ColumnReference {
            qualifier,
            alias: _,
            star: true,
        }) = ne.expression
        {
            if qualifier.is_some() {
                source_expressions.extend(
                    fieldnames_for_operator(source)
                        .filter(|(field_qualifier, _alias)| {
                            field_qualifier == &qualifier.as_deref()
                        })
                        .map(fields_to_ne),
                );
            } else {
                source_expressions.extend(fieldnames_for_operator(source).map(fields_to_ne));
            }
        } else {
            source_expressions.push(ne);
        }
    }
}

/// Function to create a reference back to the field
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
