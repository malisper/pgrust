pub const PG_CATALOG_NAMESPACE_OID: u32 = 11;
pub const PUBLIC_NAMESPACE_OID: u32 = 2200;

pub const PG_TYPE_RELATION_OID: u32 = 1247;
pub const PG_ATTRIBUTE_RELATION_OID: u32 = 1249;
pub const PG_PROC_RELATION_OID: u32 = 1255;
pub const PG_CLASS_RELATION_OID: u32 = 1259;
pub const PG_NAMESPACE_RELATION_OID: u32 = 2615;

pub const PG_NAMESPACE_ROWTYPE_OID: u32 = 0;
pub const PG_TYPE_ROWTYPE_OID: u32 = 71;
pub const PG_ATTRIBUTE_ROWTYPE_OID: u32 = 75;
pub const PG_CLASS_ROWTYPE_OID: u32 = 83;

pub const BOOL_TYPE_OID: u32 = 16;
pub const BYTEA_TYPE_OID: u32 = 17;
pub const INTERNAL_CHAR_TYPE_OID: u32 = 18;
pub const BIT_TYPE_OID: u32 = 1560;
pub const VARBIT_TYPE_OID: u32 = 1562;
pub const BOOL_ARRAY_TYPE_OID: u32 = 1000;
pub const BYTEA_ARRAY_TYPE_OID: u32 = 1001;
pub const INTERNAL_CHAR_ARRAY_TYPE_OID: u32 = 1002;
pub const BIT_ARRAY_TYPE_OID: u32 = 1561;
pub const VARBIT_ARRAY_TYPE_OID: u32 = 1563;
pub const INT8_TYPE_OID: u32 = 20;
pub const INT2_TYPE_OID: u32 = 21;
pub const INT4_TYPE_OID: u32 = 23;
pub const INT2_ARRAY_TYPE_OID: u32 = 1005;
pub const INT4_ARRAY_TYPE_OID: u32 = 1007;
pub const TEXT_TYPE_OID: u32 = 25;
pub const OID_TYPE_OID: u32 = 26;
pub const TEXT_ARRAY_TYPE_OID: u32 = 1009;
pub const BPCHAR_ARRAY_TYPE_OID: u32 = 1014;
pub const VARCHAR_ARRAY_TYPE_OID: u32 = 1015;
pub const INT8_ARRAY_TYPE_OID: u32 = 1016;
pub const FLOAT4_TYPE_OID: u32 = 700;
pub const FLOAT8_TYPE_OID: u32 = 701;
pub const FLOAT4_ARRAY_TYPE_OID: u32 = 1021;
pub const FLOAT8_ARRAY_TYPE_OID: u32 = 1022;
pub const VARCHAR_TYPE_OID: u32 = 1043;
pub const BPCHAR_TYPE_OID: u32 = 1042;
pub const TIMESTAMP_TYPE_OID: u32 = 1114;
pub const TIMESTAMP_ARRAY_TYPE_OID: u32 = 1115;
pub const NUMERIC_TYPE_OID: u32 = 1700;
pub const NUMERIC_ARRAY_TYPE_OID: u32 = 1231;
pub const JSON_TYPE_OID: u32 = 114;
pub const JSON_ARRAY_TYPE_OID: u32 = 199;
pub const OID_ARRAY_TYPE_OID: u32 = 1028;
pub const JSONB_TYPE_OID: u32 = 3802;
pub const JSONB_ARRAY_TYPE_OID: u32 = 3807;
pub const JSONPATH_TYPE_OID: u32 = 4072;
pub const JSONPATH_ARRAY_TYPE_OID: u32 = 4073;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapCatalogRelation {
    pub oid: u32,
    pub name: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BootstrapCatalogKind {
    PgNamespace,
    PgClass,
    PgAttribute,
    PgType,
}

impl BootstrapCatalogKind {
    pub const fn relation_oid(self) -> u32 {
        match self {
            Self::PgNamespace => PG_NAMESPACE_RELATION_OID,
            Self::PgClass => PG_CLASS_RELATION_OID,
            Self::PgAttribute => PG_ATTRIBUTE_RELATION_OID,
            Self::PgType => PG_TYPE_RELATION_OID,
        }
    }

    pub const fn relation_name(self) -> &'static str {
        match self {
            Self::PgNamespace => "pg_namespace",
            Self::PgClass => "pg_class",
            Self::PgAttribute => "pg_attribute",
            Self::PgType => "pg_type",
        }
    }

    pub const fn row_type_oid(self) -> u32 {
        match self {
            Self::PgNamespace => PG_NAMESPACE_ROWTYPE_OID,
            Self::PgClass => PG_CLASS_ROWTYPE_OID,
            Self::PgAttribute => PG_ATTRIBUTE_ROWTYPE_OID,
            Self::PgType => PG_TYPE_ROWTYPE_OID,
        }
    }
}

pub const CORE_BOOTSTRAP_KINDS: [BootstrapCatalogKind; 4] = [
    BootstrapCatalogKind::PgNamespace,
    BootstrapCatalogKind::PgType,
    BootstrapCatalogKind::PgAttribute,
    BootstrapCatalogKind::PgClass,
];

pub const fn bootstrap_catalog_kinds() -> [BootstrapCatalogKind; 4] {
    CORE_BOOTSTRAP_KINDS
}

pub fn bootstrap_relation_desc(kind: BootstrapCatalogKind) -> RelationDesc {
    match kind {
        BootstrapCatalogKind::PgNamespace => pg_namespace_desc(),
        BootstrapCatalogKind::PgClass => pg_class_desc(),
        BootstrapCatalogKind::PgAttribute => pg_attribute_desc(),
        BootstrapCatalogKind::PgType => pg_type_desc(),
    }
}

pub const fn bootstrap_namespace_oid() -> u32 {
    PG_CATALOG_NAMESPACE_OID
}

pub const CORE_BOOTSTRAP_RELATIONS: [BootstrapCatalogRelation; 4] = [
    BootstrapCatalogRelation {
        oid: PG_NAMESPACE_RELATION_OID,
        name: "pg_namespace",
    },
    BootstrapCatalogRelation {
        oid: PG_TYPE_RELATION_OID,
        name: "pg_type",
    },
    BootstrapCatalogRelation {
        oid: PG_ATTRIBUTE_RELATION_OID,
        name: "pg_attribute",
    },
    BootstrapCatalogRelation {
        oid: PG_CLASS_RELATION_OID,
        name: "pg_class",
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_bootstrap_relations_match_expected_oids() {
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[0].oid, PG_NAMESPACE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[1].oid, PG_TYPE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[2].oid, PG_ATTRIBUTE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[3].oid, PG_CLASS_RELATION_OID);
    }

    #[test]
    fn core_bootstrap_relation_names_are_stable() {
        let names: Vec<_> = CORE_BOOTSTRAP_RELATIONS.iter().map(|rel| rel.name).collect();
        assert_eq!(names, vec!["pg_namespace", "pg_type", "pg_attribute", "pg_class"]);
    }

    #[test]
    fn core_bootstrap_kinds_match_relation_list() {
        let pairs: Vec<_> = CORE_BOOTSTRAP_KINDS
            .iter()
            .map(|kind| (kind.relation_oid(), kind.relation_name()))
            .collect();
        let shared: Vec<_> = CORE_BOOTSTRAP_RELATIONS
            .iter()
            .map(|rel| (rel.oid, rel.name))
            .collect();
        assert_eq!(pairs, shared);
    }
}
use crate::backend::executor::RelationDesc;
use super::{pg_attribute_desc, pg_class_desc, pg_namespace_desc, pg_type_desc};
