use crate::backend::catalog::catalog::CatalogEntry;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlTypeKind;
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::catalog::{
    DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, PG_ATTRDEF_RELATION_OID,
    PG_CLASS_RELATION_OID, PG_CONSTRAINT_RELATION_OID, PG_NAMESPACE_RELATION_OID,
    PG_PROC_RELATION_OID, PG_REWRITE_RELATION_OID, PG_TYPE_RELATION_OID, PgDependRow,
};
use std::collections::BTreeSet;

pub fn sort_pg_depend_rows(rows: &mut [PgDependRow]) {
    rows.sort_by_key(|row| {
        (
            row.classid,
            row.objid,
            row.objsubid,
            row.refclassid,
            row.refobjid,
            row.refobjsubid,
            row.deptype as u32,
        )
    });
}

pub fn derived_pg_depend_rows(entry: &CatalogEntry) -> Vec<PgDependRow> {
    let mut rows = derived_relation_depend_rows(
        entry.relation_oid,
        entry.namespace_oid,
        entry.row_type_oid,
        &entry.desc,
    );
    if let Some(index_meta) = &entry.index_meta {
        rows.extend(index_meta.indkey.iter().map(|attnum| PgDependRow {
            classid: PG_CLASS_RELATION_OID,
            objid: entry.relation_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: index_meta.indrelid,
            refobjsubid: i32::from(*attnum),
            deptype: DEPENDENCY_AUTO,
        }));
    }
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn index_backed_constraint_depend_rows(
    constraint_oid: u32,
    relation_oid: u32,
    index_oid: u32,
) -> Vec<PgDependRow> {
    let mut rows = vec![
        PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: constraint_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: relation_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_AUTO,
        },
        PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: constraint_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: index_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_INTERNAL,
        },
    ];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn relation_constraint_depend_rows(constraint_oid: u32, relation_oid: u32) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_CONSTRAINT_RELATION_OID,
        objid: constraint_oid,
        objsubid: 0,
        refclassid: PG_CLASS_RELATION_OID,
        refobjid: relation_oid,
        refobjsubid: 0,
        deptype: DEPENDENCY_AUTO,
    }];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn foreign_key_constraint_depend_rows(
    constraint_oid: u32,
    child_relation_oid: u32,
    parent_relation_oid: u32,
    parent_index_oid: u32,
) -> Vec<PgDependRow> {
    let mut rows = vec![
        PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: constraint_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: child_relation_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_AUTO,
        },
        PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: constraint_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: parent_relation_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
        PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: constraint_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: parent_index_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
    ];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn inheritance_depend_rows(relation_oid: u32, parent_oids: &[u32]) -> Vec<PgDependRow> {
    let mut rows = parent_oids
        .iter()
        .copied()
        .map(|parent_oid| PgDependRow {
            classid: PG_CLASS_RELATION_OID,
            objid: relation_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: parent_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        })
        .collect::<Vec<_>>();
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn primary_key_owned_not_null_depend_rows(
    not_null_constraint_oid: u32,
    primary_constraint_oid: u32,
) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_CONSTRAINT_RELATION_OID,
        objid: not_null_constraint_oid,
        objsubid: 0,
        refclassid: PG_CONSTRAINT_RELATION_OID,
        refobjid: primary_constraint_oid,
        refobjsubid: 0,
        deptype: DEPENDENCY_INTERNAL,
    }];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn derived_relation_depend_rows(
    relation_oid: u32,
    namespace_oid: u32,
    row_type_oid: u32,
    desc: &RelationDesc,
) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_CLASS_RELATION_OID,
        objid: relation_oid,
        objsubid: 0,
        refclassid: PG_NAMESPACE_RELATION_OID,
        refobjid: namespace_oid,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }];

    if row_type_oid != 0 {
        rows.push(PgDependRow {
            classid: PG_TYPE_RELATION_OID,
            objid: row_type_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: relation_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_INTERNAL,
        });
    }

    rows.extend(desc.columns.iter().enumerate().filter_map(|(idx, column)| {
        Some(PgDependRow {
            classid: PG_ATTRDEF_RELATION_OID,
            objid: column.attrdef_oid?,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: relation_oid,
            refobjsubid: idx.saturating_add(1) as i32,
            deptype: DEPENDENCY_AUTO,
        })
    }));
    rows.extend(desc.columns.iter().enumerate().filter_map(|(idx, column)| {
        Some(PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: column.not_null_constraint_oid?,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: relation_oid,
            refobjsubid: idx.saturating_add(1) as i32,
            deptype: DEPENDENCY_AUTO,
        })
    }));
    let mut referenced_type_oids = BTreeSet::new();
    for column in &desc.columns {
        let type_oid = sql_type_oid(column.sql_type);
        if type_oid != 0 {
            referenced_type_oids.insert(type_oid);
        }
        if column.sql_type.is_array
            && matches!(
                column.sql_type.kind,
                SqlTypeKind::Composite | SqlTypeKind::Record
            )
            && column.sql_type.type_oid != 0
        {
            referenced_type_oids.insert(column.sql_type.type_oid);
        }
    }
    rows.extend(
        referenced_type_oids
            .into_iter()
            .map(|type_oid| PgDependRow {
                classid: PG_CLASS_RELATION_OID,
                objid: relation_oid,
                objsubid: 0,
                refclassid: PG_TYPE_RELATION_OID,
                refobjid: type_oid,
                refobjsubid: 0,
                deptype: DEPENDENCY_NORMAL,
            }),
    );
    rows
}

pub fn proc_depend_rows(
    proc_oid: u32,
    namespace_oid: u32,
    return_type_oid: u32,
    arg_type_oids: &[u32],
) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_PROC_RELATION_OID,
        objid: proc_oid,
        objsubid: 0,
        refclassid: PG_NAMESPACE_RELATION_OID,
        refobjid: namespace_oid,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }];
    let mut referenced_type_oids = BTreeSet::new();
    if return_type_oid != 0 {
        referenced_type_oids.insert(return_type_oid);
    }
    referenced_type_oids.extend(arg_type_oids.iter().copied().filter(|oid| *oid != 0));
    rows.extend(
        referenced_type_oids
            .into_iter()
            .map(|type_oid| PgDependRow {
                classid: PG_PROC_RELATION_OID,
                objid: proc_oid,
                objsubid: 0,
                refclassid: PG_TYPE_RELATION_OID,
                refobjid: type_oid,
                refobjsubid: 0,
                deptype: DEPENDENCY_NORMAL,
            }),
    );
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn view_rewrite_depend_rows(
    rewrite_oid: u32,
    view_relation_oid: u32,
    referenced_relation_oids: &[u32],
) -> Vec<PgDependRow> {
    rewrite_depend_rows(
        rewrite_oid,
        view_relation_oid,
        referenced_relation_oids,
        DEPENDENCY_INTERNAL,
    )
}

pub fn relation_rule_depend_rows(
    rewrite_oid: u32,
    relation_oid: u32,
    referenced_relation_oids: &[u32],
) -> Vec<PgDependRow> {
    rewrite_depend_rows(
        rewrite_oid,
        relation_oid,
        referenced_relation_oids,
        DEPENDENCY_AUTO,
    )
}

fn rewrite_depend_rows(
    rewrite_oid: u32,
    relation_oid: u32,
    referenced_relation_oids: &[u32],
    owner_deptype: char,
) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_REWRITE_RELATION_OID,
        objid: rewrite_oid,
        objsubid: 0,
        refclassid: PG_CLASS_RELATION_OID,
        refobjid: relation_oid,
        refobjsubid: 0,
        deptype: owner_deptype,
    }];
    rows.extend(
        referenced_relation_oids
            .iter()
            .copied()
            .filter(|referenced_oid| *referenced_oid != relation_oid)
            .map(|relation_oid| PgDependRow {
                classid: PG_REWRITE_RELATION_OID,
                objid: rewrite_oid,
                objsubid: 0,
                refclassid: PG_CLASS_RELATION_OID,
                refobjid: relation_oid,
                refobjsubid: 0,
                deptype: DEPENDENCY_NORMAL,
            }),
    );
    sort_pg_depend_rows(&mut rows);
    rows
}
