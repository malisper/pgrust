use std::collections::BTreeSet;

use pgrust_analyze::{
    BoundIndexRelation, BoundRelation, CatalogLookup, ReferencedForeignKeyIndexKind,
    find_referenced_foreign_key_index,
};
use pgrust_catalog_data::{CONSTRAINT_FOREIGN, PgConstraintRow};
use pgrust_nodes::parsenodes::{ForeignKeyAction, ForeignKeyMatchType, ParseError};
use pgrust_nodes::primnodes::RelationDesc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferencedPartitionForeignKeyClonePlan {
    pub constraint_name: String,
    pub local_attnums: Vec<i16>,
    pub referenced_relation_oid: u32,
    pub referenced_index_oid: u32,
    pub referenced_attnums: Vec<i16>,
    pub confupdtype: char,
    pub confdeltype: char,
    pub confmatchtype: char,
    pub confdelsetcols: Option<Vec<i16>>,
    pub conperiod: bool,
    pub condeferrable: bool,
    pub condeferred: bool,
    pub conenforced: bool,
    pub convalidated: bool,
    pub conparentid: u32,
}

pub fn foreign_key_action_code(action: ForeignKeyAction) -> char {
    match action {
        ForeignKeyAction::NoAction => 'a',
        ForeignKeyAction::Restrict => 'r',
        ForeignKeyAction::Cascade => 'c',
        ForeignKeyAction::SetNull => 'n',
        ForeignKeyAction::SetDefault => 'd',
    }
}

pub fn foreign_key_match_code(match_type: ForeignKeyMatchType) -> char {
    match match_type {
        ForeignKeyMatchType::Simple => 's',
        ForeignKeyMatchType::Full => 'f',
        ForeignKeyMatchType::Partial => 'p',
    }
}

pub fn choose_partition_clone_constraint_name(
    base: &str,
    used_names: &mut BTreeSet<String>,
) -> String {
    for suffix in 1.. {
        let candidate = format!("{base}_{suffix}");
        if used_names.insert(candidate.to_ascii_lowercase()) {
            return candidate;
        }
    }
    unreachable!("constraint name suffix space exhausted")
}

pub fn namespace_constraint_names(
    catalog: &dyn CatalogLookup,
    namespace_oid: u32,
) -> BTreeSet<String> {
    catalog
        .constraint_rows()
        .into_iter()
        .filter(|row| row.connamespace == namespace_oid)
        .map(|row| row.conname.to_ascii_lowercase())
        .collect()
}

pub fn referenced_partition_clone_used_names(
    catalog: &dyn CatalogLookup,
    parent_constraint: &PgConstraintRow,
) -> BTreeSet<String> {
    // PostgreSQL only asks ChooseConstraintName for FK partition clones after
    // the supplied name is already used on the same relation.  That chooser
    // avoids collisions across the namespace, not just the relation.
    namespace_constraint_names(catalog, parent_constraint.connamespace)
}

pub fn column_attnums_for_names(
    desc: &RelationDesc,
    columns: &[String],
) -> Result<Vec<i16>, ParseError> {
    columns
        .iter()
        .map(|column_name| {
            desc.columns
                .iter()
                .enumerate()
                .find_map(|(index, column)| {
                    (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                        .then_some(index as i16 + 1)
                })
                .ok_or_else(|| ParseError::UnknownColumn(column_name.clone()))
        })
        .collect()
}

pub fn attnums_by_parent_column_names(
    parent_desc: &RelationDesc,
    child_desc: &RelationDesc,
    parent_attnums: &[i16],
) -> Result<Vec<i16>, ParseError> {
    parent_attnums
        .iter()
        .map(|attnum| {
            let index = usize::try_from(attnum.saturating_sub(1)).map_err(|_| {
                ParseError::UnexpectedToken {
                    expected: "user column attnum",
                    actual: attnum.to_string(),
                }
            })?;
            let parent_column =
                parent_desc
                    .columns
                    .get(index)
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "user column attnum",
                        actual: attnum.to_string(),
                    })?;
            child_desc
                .columns
                .iter()
                .enumerate()
                .find_map(|(child_index, child_column)| {
                    (!child_column.dropped
                        && child_column.name.eq_ignore_ascii_case(&parent_column.name))
                    .then_some(child_index as i16 + 1)
                })
                .ok_or_else(|| ParseError::UnknownColumn(parent_column.name.clone()))
        })
        .collect()
}

pub fn is_referenced_side_foreign_key_clone(
    row: &PgConstraintRow,
    catalog: &dyn CatalogLookup,
) -> bool {
    if row.contype != CONSTRAINT_FOREIGN || row.conparentid == 0 {
        return false;
    }
    catalog
        .constraint_row_by_oid(row.conparentid)
        .is_some_and(|parent| parent.conrelid == row.conrelid)
}

pub fn can_spawn_referenced_partition_foreign_key_clone(
    row: &PgConstraintRow,
    catalog: &dyn CatalogLookup,
) -> bool {
    row.contype == CONSTRAINT_FOREIGN
        && (row.conparentid == 0 || is_referenced_side_foreign_key_clone(row, catalog))
}

pub fn foreign_key_attach_key_matches(
    child: &PgConstraintRow,
    parent: &PgConstraintRow,
    local_attnums: &[i16],
    referenced_attnums: &[i16],
    delete_set_attnums: Option<&[i16]>,
) -> bool {
    child.contype == CONSTRAINT_FOREIGN
        && child.confrelid == parent.confrelid
        && child.conkey.as_deref() == Some(local_attnums)
        && child.confkey.as_deref() == Some(referenced_attnums)
        && optional_attnums_equal(child.confdelsetcols.as_deref(), delete_set_attnums)
        && child.conperiod == parent.conperiod
}

pub fn foreign_key_attach_attributes_match(
    child: &PgConstraintRow,
    parent: &PgConstraintRow,
) -> bool {
    child.condeferrable == parent.condeferrable
        && child.condeferred == parent.condeferred
        && child.confupdtype == parent.confupdtype
        && child.confdeltype == parent.confdeltype
        && child.confmatchtype == parent.confmatchtype
}

pub fn plan_referenced_partition_foreign_key_clone(
    catalog: &dyn CatalogLookup,
    referenced_parent: &BoundRelation,
    referenced_child: &BoundRelation,
    parent_constraint: &PgConstraintRow,
    parent_referenced_attnums: &[i16],
    clone_name_base: &str,
    used_names: &mut BTreeSet<String>,
) -> Result<ReferencedPartitionForeignKeyClonePlan, ParseError> {
    let referenced_attnums = attnums_by_parent_column_names(
        &referenced_parent.desc,
        &referenced_child.desc,
        parent_referenced_attnums,
    )?;
    let referenced_index = find_referenced_foreign_key_index_for_relation(
        catalog,
        referenced_child.relation_oid,
        &referenced_attnums,
        parent_constraint.conperiod,
    )
    .ok_or_else(|| ParseError::UnexpectedToken {
        expected: "referenced UNIQUE or PRIMARY KEY index",
        actual: format!(
            "missing referenced index for partition {}",
            catalog
                .class_row_by_oid(referenced_child.relation_oid)
                .map(|row| row.relname)
                .unwrap_or_else(|| referenced_child.relation_oid.to_string())
        ),
    })?;
    let local_attnums =
        parent_constraint
            .conkey
            .clone()
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "foreign key columns",
                actual: format!("missing conkey for {}", parent_constraint.conname),
            })?;

    Ok(ReferencedPartitionForeignKeyClonePlan {
        constraint_name: choose_partition_clone_constraint_name(clone_name_base, used_names),
        local_attnums,
        referenced_relation_oid: referenced_child.relation_oid,
        referenced_index_oid: referenced_index.relation_oid,
        referenced_attnums,
        confupdtype: parent_constraint.confupdtype,
        confdeltype: parent_constraint.confdeltype,
        confmatchtype: parent_constraint.confmatchtype,
        confdelsetcols: parent_constraint.confdelsetcols.clone(),
        conperiod: parent_constraint.conperiod,
        condeferrable: parent_constraint.condeferrable,
        condeferred: parent_constraint.condeferred,
        conenforced: parent_constraint.conenforced,
        convalidated: parent_constraint.convalidated,
        conparentid: parent_constraint.oid,
    })
}

pub fn find_referenced_foreign_key_index_for_relation(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    attnums: &[i16],
    conperiod: bool,
) -> Option<BoundIndexRelation> {
    find_referenced_foreign_key_index(
        catalog,
        relation_oid,
        attnums,
        if conperiod {
            ReferencedForeignKeyIndexKind::Temporal
        } else {
            ReferencedForeignKeyIndexKind::Normal
        },
    )
}

fn optional_attnums_equal(left: Option<&[i16]>, right: Option<&[i16]>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        (None, None) => true,
        (Some(left), None) | (None, Some(left)) => left.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use pgrust_catalog_data::{BTREE_AM_OID, GIST_AM_OID, PUBLIC_NAMESPACE_OID, desc::column_desc};
    use pgrust_core::RelFileLocator;
    use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
    use pgrust_nodes::relcache::IndexRelCacheEntry;

    use super::*;

    #[derive(Clone)]
    struct TestCatalog {
        constraints: Vec<PgConstraintRow>,
        indexes: Vec<BoundIndexRelation>,
    }

    impl CatalogLookup for TestCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
            None
        }

        fn constraint_rows(&self) -> Vec<PgConstraintRow> {
            self.constraints.clone()
        }

        fn index_relations_for_heap(&self, relation_oid: u32) -> Vec<BoundIndexRelation> {
            self.indexes
                .iter()
                .filter(|index| index.index_meta.indrelid == relation_oid)
                .cloned()
                .collect()
        }
    }

    fn relation(oid: u32, names: &[&str]) -> BoundRelation {
        BoundRelation {
            rel: RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: oid,
            },
            relation_oid: oid,
            toast: None,
            namespace_oid: PUBLIC_NAMESPACE_OID,
            owner_oid: 10,
            of_type_oid: 0,
            relpersistence: 'p',
            relkind: 'r',
            relispopulated: true,
            relispartition: false,
            relpartbound: None,
            desc: RelationDesc {
                columns: names
                    .iter()
                    .map(|name| column_desc(*name, SqlType::new(SqlTypeKind::Int4), false))
                    .collect(),
            },
            partitioned_table: None,
            partition_spec: None,
        }
    }

    fn index(relation_oid: u32, index_oid: u32, indkey: Vec<i16>) -> BoundIndexRelation {
        BoundIndexRelation {
            name: format!("idx_{index_oid}"),
            rel: RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: index_oid,
            },
            relation_oid: index_oid,
            relkind: 'i',
            desc: RelationDesc {
                columns: Vec::new(),
            },
            index_meta: IndexRelCacheEntry {
                indexrelid: index_oid,
                indrelid: relation_oid,
                indnatts: indkey.len() as i16,
                indnkeyatts: indkey.len() as i16,
                indisunique: true,
                indnullsnotdistinct: false,
                indisprimary: true,
                indisexclusion: false,
                indimmediate: true,
                indisclustered: false,
                indisvalid: true,
                indcheckxmin: false,
                indisready: true,
                indislive: true,
                indisreplident: false,
                am_oid: BTREE_AM_OID,
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

    fn foreign_key_row() -> PgConstraintRow {
        PgConstraintRow {
            oid: 50,
            conname: "fk_parent_a_fkey".into(),
            connamespace: PUBLIC_NAMESPACE_OID,
            contype: CONSTRAINT_FOREIGN,
            condeferrable: true,
            condeferred: true,
            conenforced: true,
            convalidated: false,
            conrelid: 10,
            contypid: 0,
            conindid: 20,
            conparentid: 0,
            confrelid: 20,
            confupdtype: 'a',
            confdeltype: 'c',
            confmatchtype: 's',
            conkey: Some(vec![1, 2]),
            confkey: Some(vec![1, 2]),
            conpfeqop: None,
            conppeqop: None,
            conffeqop: None,
            confdelsetcols: Some(vec![1]),
            conexclop: None,
            conbin: None,
            conislocal: true,
            coninhcount: 0,
            connoinherit: false,
            conperiod: false,
        }
    }

    #[test]
    fn clone_plan_remaps_columns_and_preserves_metadata() {
        let parent = relation(20, &["a", "b"]);
        let mut child = relation(21, &["dropme", "b", "a"]);
        child.desc.columns[0].dropped = true;
        let row = foreign_key_row();
        let catalog = TestCatalog {
            constraints: vec![row.clone()],
            indexes: vec![index(21, 210, vec![3, 2])],
        };
        let mut used = BTreeSet::from(["fk_parent_a_fkey".into()]);

        let plan = plan_referenced_partition_foreign_key_clone(
            &catalog,
            &parent,
            &child,
            &row,
            &[1, 2],
            &row.conname,
            &mut used,
        )
        .unwrap();

        assert_eq!(plan.constraint_name, "fk_parent_a_fkey_1");
        assert_eq!(plan.local_attnums, vec![1, 2]);
        assert_eq!(plan.referenced_relation_oid, 21);
        assert_eq!(plan.referenced_index_oid, 210);
        assert_eq!(plan.referenced_attnums, vec![3, 2]);
        assert_eq!(plan.confupdtype, 'a');
        assert_eq!(plan.confdeltype, 'c');
        assert_eq!(plan.confmatchtype, 's');
        assert_eq!(plan.confdelsetcols, Some(vec![1]));
        assert!(!plan.conperiod);
        assert!(plan.condeferrable);
        assert!(plan.condeferred);
        assert!(plan.conenforced);
        assert!(!plan.convalidated);
        assert_eq!(plan.conparentid, row.oid);
    }

    #[test]
    fn temporal_clone_plan_uses_exclusion_index_and_preserves_period() {
        let parent = relation(20, &["a", "valid_at"]);
        let child = relation(21, &["valid_at", "a"]);
        let mut row = foreign_key_row();
        row.conperiod = true;
        row.confkey = Some(vec![1, 2]);
        let mut referenced_index = index(21, 210, vec![2, 1]);
        referenced_index.index_meta.indisunique = false;
        referenced_index.index_meta.indisprimary = false;
        referenced_index.index_meta.indisexclusion = true;
        referenced_index.index_meta.am_oid = GIST_AM_OID;
        let catalog = TestCatalog {
            constraints: vec![row.clone()],
            indexes: vec![referenced_index],
        };
        let mut used = BTreeSet::from(["fk_parent_a_fkey".into()]);

        let plan = plan_referenced_partition_foreign_key_clone(
            &catalog,
            &parent,
            &child,
            &row,
            &[1, 2],
            &row.conname,
            &mut used,
        )
        .unwrap();

        assert_eq!(plan.referenced_index_oid, 210);
        assert_eq!(plan.referenced_attnums, vec![2, 1]);
        assert!(plan.conperiod);
    }
}
