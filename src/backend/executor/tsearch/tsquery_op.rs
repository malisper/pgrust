use std::cmp::Ordering;

use crate::include::nodes::tsearch::{TsQuery, TsQueryNode};

pub(crate) fn compare_tsquery(left: &TsQuery, right: &TsQuery) -> Ordering {
    left.render().cmp(&right.render())
}

pub(crate) fn tsquery_and(left: TsQuery, right: TsQuery) -> TsQuery {
    TsQuery {
        root: TsQueryNode::And(Box::new(left.root), Box::new(right.root)),
    }
}

pub(crate) fn tsquery_or(left: TsQuery, right: TsQuery) -> TsQuery {
    TsQuery {
        root: TsQueryNode::Or(Box::new(left.root), Box::new(right.root)),
    }
}

pub(crate) fn tsquery_not(query: TsQuery) -> TsQuery {
    TsQuery {
        root: TsQueryNode::Not(Box::new(query.root)),
    }
}
