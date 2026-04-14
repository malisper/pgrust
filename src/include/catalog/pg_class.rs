use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID, BootstrapCatalogKind, HEAP_TABLE_AM_OID,
    PG_CATALOG_NAMESPACE_OID, bootstrap_relation_desc,
};

#[derive(Debug, Clone, PartialEq)]
pub struct PgClassRow {
    pub oid: u32,
    pub relname: String,
    pub relnamespace: u32,
    pub reltype: u32,
    pub relowner: u32,
    pub relam: u32,
    pub reltablespace: u32,
    pub relfilenode: u32,
    pub reltoastrelid: u32,
    pub relpersistence: char,
    pub relkind: char,
    pub relnatts: i16,
    pub relpages: i32,
    pub reltuples: f64,
}

pub fn pg_class_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("relname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("relnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("reltype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("relowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("relam", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("reltablespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("relfilenode", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("reltoastrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "relpersistence",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("relkind", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("relnatts", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("relpages", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("reltuples", SqlType::new(SqlTypeKind::Float4), false),
        ],
    }
}

pub const fn relam_for_relkind(relkind: char) -> u32 {
    match relkind {
        'i' => BTREE_AM_OID,
        'v' => 0,
        _ => HEAP_TABLE_AM_OID,
    }
}

pub fn bootstrap_pg_class_rows() -> [PgClassRow; 20] {
    [
        bootstrap_pg_class_row(BootstrapCatalogKind::PgNamespace),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgType),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgProc),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgLanguage),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgOperator),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAttribute),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgClass),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAuthId),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAuthMembers),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgCollation),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgDatabase),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgTablespace),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAm),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAttrdef),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgCast),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgConstraint),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgDepend),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgIndex),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgRewrite),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgStatistic),
    ]
}

fn bootstrap_pg_class_row(kind: BootstrapCatalogKind) -> PgClassRow {
    PgClassRow {
        oid: kind.relation_oid(),
        relname: kind.relation_name().into(),
        relnamespace: PG_CATALOG_NAMESPACE_OID,
        reltype: kind.row_type_oid(),
        relowner: BOOTSTRAP_SUPERUSER_OID,
        relam: relam_for_relkind('r'),
        reltablespace: 0,
        relfilenode: kind.relation_oid(),
        reltoastrelid: 0,
        relpersistence: 'p',
        relkind: 'r',
        relnatts: bootstrap_relation_desc(kind).columns.len() as i16,
        relpages: 0,
        reltuples: 0.0,
    }
}
