use std::collections::HashMap;

use crate::backend::optimizer::pathnodes::expr_sql_type;
use crate::backend::parser::SqlType;
use crate::backend::parser::analyze::is_binary_coercible_type;
use crate::include::nodes::pathnodes::PathTarget;
use crate::include::nodes::primnodes::{AttrNumber, Expr, Var};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SimpleVarKey {
    varno: usize,
    varattno: AttrNumber,
    varlevelsup: usize,
    vartype: SqlType,
}

pub(crate) fn simple_var_key(expr: &Expr) -> Option<SimpleVarKey> {
    match expr {
        Expr::Var(Var {
            varno,
            varattno,
            varlevelsup,
            vartype,
        }) => Some(SimpleVarKey {
            varno: *varno,
            varattno: *varattno,
            varlevelsup: *varlevelsup,
            vartype: *vartype,
        }),
        _ => None,
    }
}

pub(crate) fn strip_binary_coercible_casts(expr: &Expr) -> Expr {
    match expr {
        Expr::Cast(inner, target_type)
            if is_binary_coercible_type(expr_sql_type(inner), *target_type) =>
        {
            strip_binary_coercible_casts(inner)
        }
        other => other.clone(),
    }
}

pub(crate) struct IndexedPathTarget<'a> {
    target: &'a PathTarget,
    by_sortgroupref: HashMap<usize, usize>,
    by_simple_var: HashMap<SimpleVarKey, usize>,
}

impl<'a> IndexedPathTarget<'a> {
    pub(crate) fn new(target: &'a PathTarget) -> Self {
        let mut by_sortgroupref = HashMap::new();
        let mut by_simple_var = HashMap::new();
        for (index, expr) in target.exprs.iter().enumerate() {
            if let Some(sortgroupref) = target.sortgrouprefs.get(index).copied()
                && sortgroupref != 0
            {
                by_sortgroupref.entry(sortgroupref).or_insert(index);
            }
            if let Some(key) = simple_var_key(expr) {
                by_simple_var.entry(key).or_insert(index);
            }
        }
        Self {
            target,
            by_sortgroupref,
            by_simple_var,
        }
    }

    pub(crate) fn index_for_sortgroupref(&self, sortgroupref: usize) -> Option<usize> {
        (sortgroupref != 0)
            .then(|| self.by_sortgroupref.get(&sortgroupref).copied())
            .flatten()
    }

    pub(crate) fn match_index(&self, expr: &Expr, sortgroupref: usize) -> Option<usize> {
        self.index_for_sortgroupref(sortgroupref)
            .or_else(|| simple_var_key(expr).and_then(|key| self.by_simple_var.get(&key).copied()))
            .or_else(|| {
                let stripped = strip_binary_coercible_casts(expr);
                self.target
                    .exprs
                    .iter()
                    .position(|candidate| strip_binary_coercible_casts(candidate) == stripped)
            })
    }

    pub(crate) fn matched_expr(&self, expr: &Expr, sortgroupref: usize) -> Option<Expr> {
        self.match_index(expr, sortgroupref)
            .and_then(|index| self.target.exprs.get(index).cloned())
    }
}
