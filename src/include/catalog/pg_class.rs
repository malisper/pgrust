use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    ACLITEM_ARRAY_TYPE_OID, ACLITEM_TYPE_OID, BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID,
    BootstrapCatalogKind, HEAP_TABLE_AM_OID, PG_CATALOG_NAMESPACE_OID, bootstrap_relation_desc,
};

#[derive(Debug, Clone, PartialEq)]
pub struct PgClassRow {
    pub oid: u32,
    pub relname: String,
    pub relnamespace: u32,
    pub reltype: u32,
    pub relowner: u32,
    pub relam: u32,
    pub relfilenode: u32,
    pub reltablespace: u32,
    pub relpages: i32,
    pub reltuples: f64,
    pub relallvisible: i32,
    pub relallfrozen: i32,
    pub reltoastrelid: u32,
    pub relhasindex: bool,
    pub relpersistence: char,
    pub relkind: char,
    pub relnatts: i16,
    pub relhassubclass: bool,
    pub relhastriggers: bool,
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    pub relispopulated: bool,
    pub relispartition: bool,
    pub relfrozenxid: u32,
    pub relpartbound: Option<String>,
    pub reloptions: Option<Vec<String>>,
    pub relacl: Option<Vec<String>>,
    pub relreplident: char,
    pub reloftype: u32,
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
            column_desc("relfilenode", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("reltablespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("relpages", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("reltuples", SqlType::new(SqlTypeKind::Float4), false),
            column_desc("relallvisible", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("relallfrozen", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("reltoastrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("relhasindex", SqlType::new(SqlTypeKind::Bool), false),
            column_desc(
                "relpersistence",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("relkind", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("relnatts", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("relhassubclass", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("relhastriggers", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("relrowsecurity", SqlType::new(SqlTypeKind::Bool), false),
            column_desc(
                "relforcerowsecurity",
                SqlType::new(SqlTypeKind::Bool),
                false,
            ),
            column_desc("relispopulated", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("relispartition", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("relfrozenxid", SqlType::new(SqlTypeKind::Xid), false),
            column_desc("relpartbound", SqlType::new(SqlTypeKind::PgNodeTree), true),
            column_desc(
                "reloptions",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
            column_desc(
                "relacl",
                SqlType::array_of(
                    SqlType::new(SqlTypeKind::Text).with_identity(ACLITEM_TYPE_OID, 0),
                )
                .with_identity(ACLITEM_ARRAY_TYPE_OID, 0),
                true,
            ),
            column_desc(
                "relreplident",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("reloftype", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub const fn relkind_has_storage(relkind: char) -> bool {
    !matches!(relkind, 'v' | 'c' | 'p' | 'I')
}

pub const fn relkind_is_analyzable(relkind: char) -> bool {
    matches!(relkind, 'r' | 'm' | 'p')
}

pub const fn relam_for_relkind(relkind: char) -> u32 {
    match relkind {
        'i' | 'I' => BTREE_AM_OID,
        _ if relkind_has_storage(relkind) => HEAP_TABLE_AM_OID,
        _ => 0,
    }
}

pub fn bootstrap_pg_class_rows() -> [PgClassRow; 44] {
    [
        bootstrap_pg_class_row(BootstrapCatalogKind::PgNamespace),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgType),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgProc),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgDefaultAcl),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgLanguage),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgOperator),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAttribute),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgClass),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAuthId),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAuthMembers),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgCollation),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgLargeobject),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgDatabase),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgExtension),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgEventTrigger),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgTablespace),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgShdepend),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgShdescription),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgReplicationOrigin),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAm),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgTransform),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAttrdef),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgCast),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgConstraint),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgConversion),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgDepend),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgForeignDataWrapper),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgForeignServer),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgIndex),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgInherits),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgPartitionedTable),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgRewrite),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgSequence),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgStatistic),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgStatisticExt),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgStatisticExtData),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgTrigger),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgPolicy),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgAggregate),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgPublication),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgPublicationRel),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgPublicationNamespace),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgSubscription),
        bootstrap_pg_class_row(BootstrapCatalogKind::PgParameterAcl),
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
        relfilenode: kind.relation_oid(),
        reltablespace: 0,
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        reltoastrelid: kind.toast_relation_oid(),
        relhasindex: false,
        relpersistence: 'p',
        relkind: 'r',
        relnatts: bootstrap_relation_desc(kind).columns.len() as i16,
        relhassubclass: false,
        relhastriggers: false,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relispopulated: true,
        relispartition: false,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
        relpartbound: None,
        reloptions: None,
        relacl: None,
        relreplident: 'd',
        reloftype: 0,
    }
}
