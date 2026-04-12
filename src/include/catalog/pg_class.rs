use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BootstrapCatalogKind, HEAP_TABLE_AM_OID, BTREE_AM_OID, PG_CATALOG_NAMESPACE_OID,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgClassRow {
    pub oid: u32,
    pub relname: String,
    pub relnamespace: u32,
    pub reltype: u32,
    pub relam: u32,
    pub relfilenode: u32,
    pub relpersistence: char,
    pub relkind: char,
}

pub fn pg_class_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("relname", SqlType::new(SqlTypeKind::Text), false),
            column_desc("relnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("reltype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("relam", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("relfilenode", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("relpersistence", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("relkind", SqlType::new(SqlTypeKind::InternalChar), false),
        ],
    }
}

pub const fn relam_for_relkind(relkind: char) -> u32 {
    match relkind {
        'i' => BTREE_AM_OID,
        _ => HEAP_TABLE_AM_OID,
    }
}

pub fn bootstrap_pg_class_rows() -> [PgClassRow; 10] {
    [
        bootstrap_pg_class_row(BootstrapCatalogKind::PgNamespace),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgType),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAttribute),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgClass),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgDatabase),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgTablespace),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAm),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAttrdef),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgDepend),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgIndex),
    ]
}

fn bootstrap_pg_class_row(kind: BootstrapCatalogKind) -> PgClassRow {
    PgClassRow {
        oid: kind.relation_oid(),
        relname: kind.relation_name().into(),
        relnamespace: PG_CATALOG_NAMESPACE_OID,
        reltype: kind.row_type_oid(),
        relam: relam_for_relkind('r'),
        relfilenode: kind.relation_oid(),
        relpersistence: 'p',
        relkind: 'r',
    }
}
