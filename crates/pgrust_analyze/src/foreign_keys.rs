use std::collections::BTreeSet;

use pgrust_catalog_data::BTREE_AM_OID;

use crate::{BoundIndexRelation, CatalogLookup};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferencedForeignKeyIndexKind {
    Normal,
    Temporal,
}

pub fn find_referenced_foreign_key_index(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    attnums: &[i16],
    kind: ReferencedForeignKeyIndexKind,
) -> Option<BoundIndexRelation> {
    catalog
        .index_relations_for_heap(relation_oid)
        .into_iter()
        .find(|index| referenced_foreign_key_index_matches(index, attnums, kind))
}

pub fn referenced_foreign_key_index_matches(
    index: &BoundIndexRelation,
    attnums: &[i16],
    kind: ReferencedForeignKeyIndexKind,
) -> bool {
    if !index.index_meta.indisvalid
        || !index.index_meta.indisready
        || !index.index_meta.indimmediate
        || index
            .index_meta
            .indpred
            .as_deref()
            .is_some_and(|pred| !pred.is_empty())
        || index
            .index_meta
            .indexprs
            .as_deref()
            .is_some_and(|exprs| !exprs.is_empty())
    {
        return false;
    }

    let Some(key_attnums) = index_key_attnums(index) else {
        return false;
    };
    if !attnum_key_columns_match_as_set(&key_attnums, attnums) {
        return false;
    }

    match kind {
        ReferencedForeignKeyIndexKind::Normal => {
            index.index_meta.indisunique
                && !index.index_meta.indisexclusion
                && index.index_meta.am_oid == BTREE_AM_OID
        }
        ReferencedForeignKeyIndexKind::Temporal => {
            index.index_meta.indisexclusion
                && attnums.last().zip(key_attnums.last()).is_some_and(
                    |(period_attnum, index_period_attnum)| period_attnum == index_period_attnum,
                )
        }
    }
}

pub fn index_key_attnums(index: &BoundIndexRelation) -> Option<Vec<i16>> {
    let key_count = usize::try_from(index.index_meta.indnkeyatts.max(0)).ok()?;
    if key_count > index.index_meta.indkey.len() {
        return None;
    }
    Some(
        index
            .index_meta
            .indkey
            .iter()
            .take(key_count)
            .copied()
            .collect(),
    )
}

pub fn attnum_key_columns_match_as_set(left: &[i16], right: &[i16]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let left = left.iter().copied().collect::<BTreeSet<_>>();
    let right = right.iter().copied().collect::<BTreeSet<_>>();
    left.len() == right.len() && left == right
}

#[cfg(test)]
mod tests {
    use pgrust_core::RelFileLocator;
    use pgrust_nodes::relcache::IndexRelCacheEntry;

    use super::*;

    fn index(
        indkey: Vec<i16>,
        indisunique: bool,
        indisexclusion: bool,
        am_oid: u32,
    ) -> BoundIndexRelation {
        BoundIndexRelation {
            name: "idx".into(),
            rel: RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: 0,
            },
            relation_oid: 1,
            relkind: 'i',
            desc: pgrust_nodes::primnodes::RelationDesc {
                columns: Vec::new(),
            },
            index_meta: IndexRelCacheEntry {
                indexrelid: 1,
                indrelid: 2,
                indnatts: indkey.len() as i16,
                indnkeyatts: indkey.len() as i16,
                indisunique,
                indnullsnotdistinct: false,
                indisprimary: false,
                indisexclusion,
                indimmediate: true,
                indisclustered: false,
                indisvalid: true,
                indcheckxmin: false,
                indisready: true,
                indislive: true,
                indisreplident: false,
                am_oid,
                am_handler_oid: None,
                indkey,
                indclass: Vec::new(),
                indclass_options: Vec::new(),
                indcollation: Vec::new(),
                indoption: Vec::new(),
                opfamily_oids: Vec::new(),
                opcintype_oids: Vec::new(),
                opckeytype_oids: Vec::new(),
                amop_entries: Vec::new(),
                amproc_entries: Vec::new(),
                indexprs: None,
                indpred: None,
                rd_indexprs: None,
                rd_indpred: None,
                btree_options: None,
                brin_options: None,
                gist_options: None,
                gin_options: None,
                hash_options: None,
            },
            index_exprs: Vec::new(),
            index_predicate: None,
            constraint_oid: None,
            constraint_name: None,
            constraint_deferrable: false,
            constraint_initially_deferred: false,
        }
    }

    #[test]
    fn normal_fk_accepts_btree_unique_index() {
        let index = index(vec![2, 1], true, false, BTREE_AM_OID);
        assert!(referenced_foreign_key_index_matches(
            &index,
            &[1, 2],
            ReferencedForeignKeyIndexKind::Normal
        ));
    }

    #[test]
    fn temporal_fk_accepts_exclusion_index_with_period_last() {
        let index = index(vec![1, 2], false, true, pgrust_catalog_data::GIST_AM_OID);
        assert!(referenced_foreign_key_index_matches(
            &index,
            &[1, 2],
            ReferencedForeignKeyIndexKind::Temporal
        ));
    }

    #[test]
    fn temporal_fk_rejects_when_period_is_not_last_index_key() {
        let index = index(vec![1, 2], false, true, pgrust_catalog_data::GIST_AM_OID);
        assert!(!referenced_foreign_key_index_matches(
            &index,
            &[2, 1],
            ReferencedForeignKeyIndexKind::Temporal
        ));
    }

    #[test]
    fn referenced_fk_index_rejects_unusable_indexes() {
        let mut index = index(vec![1], true, false, BTREE_AM_OID);

        index.index_meta.indisvalid = false;
        assert!(!referenced_foreign_key_index_matches(
            &index,
            &[1],
            ReferencedForeignKeyIndexKind::Normal
        ));

        index.index_meta.indisvalid = true;
        index.index_meta.indisready = false;
        assert!(!referenced_foreign_key_index_matches(
            &index,
            &[1],
            ReferencedForeignKeyIndexKind::Normal
        ));

        index.index_meta.indisready = true;
        index.index_meta.indimmediate = false;
        assert!(!referenced_foreign_key_index_matches(
            &index,
            &[1],
            ReferencedForeignKeyIndexKind::Normal
        ));

        index.index_meta.indimmediate = true;
        index.index_meta.indpred = Some("a > 0".into());
        assert!(!referenced_foreign_key_index_matches(
            &index,
            &[1],
            ReferencedForeignKeyIndexKind::Normal
        ));

        index.index_meta.indpred = None;
        index.index_meta.indexprs = Some("expr".into());
        assert!(!referenced_foreign_key_index_matches(
            &index,
            &[1],
            ReferencedForeignKeyIndexKind::Normal
        ));
    }
}
