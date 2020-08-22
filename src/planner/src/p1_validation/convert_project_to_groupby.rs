use crate::utils::expr::contains_aggregate;
use ast::rel::logical::*;

/// Detects projects using aggregate functions and turns them into a group by.
pub(super) fn project_to_groupby(operator: &mut LogicalOperator) {
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
