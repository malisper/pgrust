use crate::backend::executor::RelationDesc;
use crate::include::catalog::{
    DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, PG_ATTRDEF_RELATION_OID,
    PG_CLASS_RELATION_OID, PG_NAMESPACE_RELATION_OID, PG_TYPE_RELATION_OID, PgDependRow,
};

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

pub fn derived_pg_depend_rows(
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
    sort_pg_depend_rows(&mut rows);
    rows
}
