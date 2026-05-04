use crate::bootstrap::{PG_AUTHID_RELATION_OID, PG_POLICY_RELATION_OID};
use crate::desc::column_desc;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

pub const SHARED_DEPENDENCY_OWNER: char = 'o';
pub const SHARED_DEPENDENCY_ACL: char = 'a';
pub const SHARED_DEPENDENCY_INITACL: char = 'i';
pub const SHARED_DEPENDENCY_POLICY: char = 'r';
pub const SHARED_DEPENDENCY_TABLESPACE: char = 't';

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgShdependRow {
    pub dbid: u32,
    pub classid: u32,
    pub objid: u32,
    pub objsubid: i32,
    pub refclassid: u32,
    pub refobjid: u32,
    pub deptype: char,
}

pub fn sort_pg_shdepend_rows(rows: &mut [PgShdependRow]) {
    rows.sort_by_key(|row| {
        (
            row.dbid,
            row.classid,
            row.objid,
            row.objsubid,
            row.refclassid,
            row.refobjid,
            row.deptype as u32,
        )
    });
}

pub fn policy_shdepend_rows(dbid: u32, policy_oid: u32, role_oids: &[u32]) -> Vec<PgShdependRow> {
    let mut rows = role_oids
        .iter()
        .copied()
        .filter(|role_oid| *role_oid != 0)
        .map(|role_oid| PgShdependRow {
            dbid,
            classid: PG_POLICY_RELATION_OID,
            objid: policy_oid,
            objsubid: 0,
            refclassid: PG_AUTHID_RELATION_OID,
            refobjid: role_oid,
            deptype: SHARED_DEPENDENCY_POLICY,
        })
        .collect::<Vec<_>>();
    sort_pg_shdepend_rows(&mut rows);
    rows.dedup();
    rows
}

pub fn pg_shdepend_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("dbid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("classid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("objid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("objsubid", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("refclassid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("refobjid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("deptype", SqlType::new(SqlTypeKind::InternalChar), false),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_shdepend_desc_contains_dependency_columns() {
        let desc = pg_shdepend_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "dbid",
                "classid",
                "objid",
                "objsubid",
                "refclassid",
                "refobjid",
                "deptype",
            ]
        );
    }
}
