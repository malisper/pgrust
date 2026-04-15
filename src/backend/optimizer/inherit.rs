use crate::backend::parser::CatalogLookup;
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{
    AppendRelInfo, PlannerInfo, RelOptInfo, RelOptKind, RestrictInfo,
};
use crate::include::nodes::primnodes::{Expr, RelationDesc, Var};

use super::pathnodes::rewrite_expr_against_layout;

pub(super) fn expand_inherited_rtentries(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) {
    let original_len = root.parse.rtable.len();
    for parent_rtindex in 1..=original_len {
        let Some(parent_rte) = root.parse.rtable.get(parent_rtindex - 1).cloned() else {
            continue;
        };
        let RangeTblEntryKind::Relation {
            relation_oid,
            relkind,
            ..
        } = parent_rte.kind
        else {
            continue;
        };
        if !parent_rte.inh || relkind != 'r' {
            continue;
        }

        let inheritors = catalog.find_all_inheritors(relation_oid);
        if inheritors.len() <= 1 {
            continue;
        }
        let parent_restrictinfo = root
            .simple_rel_array
            .get(parent_rtindex)
            .and_then(Option::as_ref)
            .map(|rel| rel.baserestrictinfo.clone())
            .unwrap_or_default();

        for child_oid in inheritors.into_iter().filter(|oid| *oid != relation_oid) {
            let Some(child) = catalog.relation_by_oid(child_oid) else {
                continue;
            };
            let child_rtindex = root.parse.rtable.len() + 1;
            let translated_vars =
                translate_parent_vars_to_child(&parent_rte.desc, child_rtindex, &child.desc);
            let child_rte = RangeTblEntry {
                alias: None,
                desc: child.desc.clone(),
                inh: false,
                kind: RangeTblEntryKind::Relation {
                    rel: child.rel,
                    relation_oid: child.relation_oid,
                    relkind: child.relkind,
                    toast: child.toast,
                },
            };
            root.parse.rtable.push(child_rte.clone());
            root.simple_rel_array
                .push(Some(RelOptInfo::from_rte(child_rtindex, &child_rte)));
            root.append_rel_infos.push(Some(AppendRelInfo {
                parent_relid: parent_rtindex,
                child_relid: child_rtindex,
                translated_vars: translated_vars.clone(),
            }));
            if let Some(rel) = root
                .simple_rel_array
                .get_mut(child_rtindex)
                .and_then(Option::as_mut)
            {
                rel.reloptkind = RelOptKind::OtherMemberRel;
                rel.baserestrictinfo = parent_restrictinfo
                    .iter()
                    .map(|restrict| RestrictInfo {
                        clause: rewrite_expr_against_layout(
                            restrict.clause.clone(),
                            &translated_vars,
                        ),
                        required_relids: vec![child_rtindex],
                        is_pushed_down: restrict.is_pushed_down,
                    })
                    .collect();
            }
        }
    }
}

pub(super) fn append_child_rtindexes(root: &PlannerInfo, parent_rtindex: usize) -> Vec<usize> {
    root.append_rel_infos
        .iter()
        .enumerate()
        .skip(1)
        .filter_map(|(rtindex, info)| {
            info.as_ref()
                .filter(|info| info.parent_relid == parent_rtindex)
                .map(|_| rtindex)
        })
        .collect()
}

pub(super) fn append_translation(
    root: &PlannerInfo,
    child_rtindex: usize,
) -> Option<&AppendRelInfo> {
    root.append_rel_infos
        .get(child_rtindex)
        .and_then(Option::as_ref)
}

fn translate_parent_vars_to_child(
    parent_desc: &RelationDesc,
    child_rtindex: usize,
    child_desc: &RelationDesc,
) -> Vec<Expr> {
    parent_desc
        .columns
        .iter()
        .map(|parent_column| {
            child_desc
                .columns
                .iter()
                .enumerate()
                .find(|(_, child_column)| {
                    !child_column.dropped
                        && child_column.name.eq_ignore_ascii_case(&parent_column.name)
                        && child_column.sql_type == parent_column.sql_type
                })
                .map(|(index, child_column)| {
                    Expr::Var(Var {
                        varno: child_rtindex,
                        varattno: index + 1,
                        varlevelsup: 0,
                        vartype: child_column.sql_type,
                    })
                })
                .unwrap_or(Expr::Const(Value::Null))
        })
        .collect()
}
