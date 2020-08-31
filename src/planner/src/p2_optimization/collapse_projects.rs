use crate::utils::expr::inline_expression;
use ast::rel::logical::LogicalOperator;

/// Walks the tree collapsing multiple projects etc into one
pub fn collapse_projects(query: &mut LogicalOperator) {
    for child in query.children_mut() {
        collapse_projects(child);
    }
    if let LogicalOperator::Project(outer) = query {
        if let LogicalOperator::Project(inner) = outer.source.as_mut() {
            // We'll just inline all the inner into the outer and do away with the inner.
            let exprs: Vec<_> = inner.expressions.iter().map(|ne| &ne.expression).collect();
            for expr in &mut outer.expressions {
                inline_expression(&mut expr.expression, &exprs)
            }

            let mut source = LogicalOperator::default();
            std::mem::swap(&mut source, inner.source.as_mut());
            outer.source = Box::from(source);
        }
    }
}
