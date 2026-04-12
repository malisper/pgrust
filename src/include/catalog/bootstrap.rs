pub const PG_CATALOG_NAMESPACE_OID: u32 = 11;
pub const PUBLIC_NAMESPACE_OID: u32 = 2200;

pub const PG_TYPE_RELATION_OID: u32 = 1247;
pub const PG_DATABASE_ROWTYPE_OID: u32 = 1248;
pub const PG_ATTRIBUTE_RELATION_OID: u32 = 1249;
pub const PG_PROC_RELATION_OID: u32 = 1255;
pub const PG_PROC_ROWTYPE_OID: u32 = 81;
pub const PG_CLASS_RELATION_OID: u32 = 1259;
pub const PG_AUTHID_RELATION_OID: u32 = 1260;
pub const PG_AUTH_MEMBERS_RELATION_OID: u32 = 1261;
pub const PG_DATABASE_RELATION_OID: u32 = 1262;
pub const PG_COLLATION_RELATION_OID: u32 = 3456;
pub const PG_TABLESPACE_RELATION_OID: u32 = 1213;
pub const PG_AM_RELATION_OID: u32 = 2601;
pub const PG_AMOP_RELATION_OID: u32 = 2602;
pub const PG_AMPROC_RELATION_OID: u32 = 2603;
pub const PG_ATTRDEF_RELATION_OID: u32 = 2604;
pub const PG_CAST_RELATION_OID: u32 = 2605;
pub const PG_CONSTRAINT_RELATION_OID: u32 = 2606;
pub const PG_DEPEND_RELATION_OID: u32 = 2608;
pub const PG_INDEX_RELATION_OID: u32 = 2610;
pub const PG_LANGUAGE_RELATION_OID: u32 = 2612;
pub const PG_NAMESPACE_RELATION_OID: u32 = 2615;
pub const PG_OPCLASS_RELATION_OID: u32 = 2616;
pub const PG_OPERATOR_RELATION_OID: u32 = 2617;
pub const PG_OPFAMILY_RELATION_OID: u32 = 2753;

pub const PG_NAMESPACE_ROWTYPE_OID: u32 = 0;
pub const PG_TYPE_ROWTYPE_OID: u32 = 71;
pub const PG_ATTRIBUTE_ROWTYPE_OID: u32 = 75;
pub const PG_CLASS_ROWTYPE_OID: u32 = 83;
pub const PG_AM_ROWTYPE_OID: u32 = 0;
pub const PG_ATTRDEF_ROWTYPE_OID: u32 = 0;
pub const PG_DEPEND_ROWTYPE_OID: u32 = 0;
pub const PG_INDEX_ROWTYPE_OID: u32 = 0;

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
pub const INT2VECTOR_TYPE_OID: u32 = 22;
pub const INT4_TYPE_OID: u32 = 23;
pub const INT2_ARRAY_TYPE_OID: u32 = 1005;
pub const INT4_ARRAY_TYPE_OID: u32 = 1007;
pub const TEXT_TYPE_OID: u32 = 25;
pub const OID_TYPE_OID: u32 = 26;
pub const OIDVECTOR_TYPE_OID: u32 = 30;
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
pub const PG_NODE_TREE_TYPE_OID: u32 = 194;
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
    PgProc,
    PgLanguage,
    PgOperator,
    PgDatabase,
    PgAuthId,
    PgAuthMembers,
    PgCollation,
    PgTablespace,
    PgAm,
    PgAmop,
    PgAmproc,
    PgAttrdef,
    PgCast,
    PgConstraint,
    PgDepend,
    PgIndex,
    PgOpclass,
    PgOpfamily,
}

impl BootstrapCatalogKind {
    pub const fn relation_oid(self) -> u32 {
        match self {
            Self::PgNamespace => PG_NAMESPACE_RELATION_OID,
            Self::PgClass => PG_CLASS_RELATION_OID,
            Self::PgAttribute => PG_ATTRIBUTE_RELATION_OID,
            Self::PgType => PG_TYPE_RELATION_OID,
            Self::PgProc => PG_PROC_RELATION_OID,
            Self::PgLanguage => PG_LANGUAGE_RELATION_OID,
            Self::PgOperator => PG_OPERATOR_RELATION_OID,
            Self::PgDatabase => PG_DATABASE_RELATION_OID,
            Self::PgAuthId => PG_AUTHID_RELATION_OID,
            Self::PgAuthMembers => PG_AUTH_MEMBERS_RELATION_OID,
            Self::PgCollation => PG_COLLATION_RELATION_OID,
            Self::PgTablespace => PG_TABLESPACE_RELATION_OID,
            Self::PgAm => PG_AM_RELATION_OID,
            Self::PgAmop => PG_AMOP_RELATION_OID,
            Self::PgAmproc => PG_AMPROC_RELATION_OID,
            Self::PgAttrdef => PG_ATTRDEF_RELATION_OID,
            Self::PgCast => PG_CAST_RELATION_OID,
            Self::PgConstraint => PG_CONSTRAINT_RELATION_OID,
            Self::PgDepend => PG_DEPEND_RELATION_OID,
            Self::PgIndex => PG_INDEX_RELATION_OID,
            Self::PgOpclass => PG_OPCLASS_RELATION_OID,
            Self::PgOpfamily => PG_OPFAMILY_RELATION_OID,
        }
    }

    pub const fn relation_name(self) -> &'static str {
        match self {
            Self::PgNamespace => "pg_namespace",
            Self::PgClass => "pg_class",
            Self::PgAttribute => "pg_attribute",
            Self::PgType => "pg_type",
            Self::PgProc => "pg_proc",
            Self::PgLanguage => "pg_language",
            Self::PgOperator => "pg_operator",
            Self::PgDatabase => "pg_database",
            Self::PgAuthId => "pg_authid",
            Self::PgAuthMembers => "pg_auth_members",
            Self::PgCollation => "pg_collation",
            Self::PgTablespace => "pg_tablespace",
            Self::PgAm => "pg_am",
            Self::PgAmop => "pg_amop",
            Self::PgAmproc => "pg_amproc",
            Self::PgAttrdef => "pg_attrdef",
            Self::PgCast => "pg_cast",
            Self::PgConstraint => "pg_constraint",
            Self::PgDepend => "pg_depend",
            Self::PgIndex => "pg_index",
            Self::PgOpclass => "pg_opclass",
            Self::PgOpfamily => "pg_opfamily",
        }
    }

    pub const fn row_type_oid(self) -> u32 {
        match self {
            Self::PgNamespace => PG_NAMESPACE_ROWTYPE_OID,
            Self::PgClass => PG_CLASS_ROWTYPE_OID,
            Self::PgAttribute => PG_ATTRIBUTE_ROWTYPE_OID,
            Self::PgType => PG_TYPE_ROWTYPE_OID,
            Self::PgProc => PG_PROC_ROWTYPE_OID,
            Self::PgLanguage => 0,
            Self::PgOperator => 0,
            Self::PgDatabase => PG_DATABASE_ROWTYPE_OID,
            Self::PgAuthId => 0,
            Self::PgAuthMembers => 0,
            Self::PgCollation => 0,
            Self::PgTablespace => 0,
            Self::PgAm => PG_AM_ROWTYPE_OID,
            Self::PgAmop => 0,
            Self::PgAmproc => 0,
            Self::PgAttrdef => PG_ATTRDEF_ROWTYPE_OID,
            Self::PgCast => 0,
            Self::PgConstraint => 0,
            Self::PgDepend => PG_DEPEND_ROWTYPE_OID,
            Self::PgIndex => PG_INDEX_ROWTYPE_OID,
            Self::PgOpclass => 0,
            Self::PgOpfamily => 0,
        }
    }
}

pub const CORE_BOOTSTRAP_KINDS: [BootstrapCatalogKind; 22] = [
    BootstrapCatalogKind::PgNamespace,
    BootstrapCatalogKind::PgType,
    BootstrapCatalogKind::PgProc,
    BootstrapCatalogKind::PgLanguage,
    BootstrapCatalogKind::PgOperator,
    BootstrapCatalogKind::PgOpfamily,
    BootstrapCatalogKind::PgOpclass,
    BootstrapCatalogKind::PgAmop,
    BootstrapCatalogKind::PgAmproc,
    BootstrapCatalogKind::PgAttribute,
    BootstrapCatalogKind::PgClass,
    BootstrapCatalogKind::PgAuthId,
    BootstrapCatalogKind::PgAuthMembers,
    BootstrapCatalogKind::PgCollation,
    BootstrapCatalogKind::PgDatabase,
    BootstrapCatalogKind::PgTablespace,
    BootstrapCatalogKind::PgAm,
    BootstrapCatalogKind::PgAttrdef,
    BootstrapCatalogKind::PgCast,
    BootstrapCatalogKind::PgConstraint,
    BootstrapCatalogKind::PgDepend,
    BootstrapCatalogKind::PgIndex,
];

pub const fn bootstrap_catalog_kinds() -> [BootstrapCatalogKind; 22] {
    CORE_BOOTSTRAP_KINDS
}

pub fn bootstrap_relation_desc(kind: BootstrapCatalogKind) -> RelationDesc {
    match kind {
        BootstrapCatalogKind::PgNamespace => pg_namespace_desc(),
        BootstrapCatalogKind::PgClass => pg_class_desc(),
        BootstrapCatalogKind::PgAttribute => pg_attribute_desc(),
        BootstrapCatalogKind::PgType => pg_type_desc(),
        BootstrapCatalogKind::PgProc => pg_proc_desc(),
        BootstrapCatalogKind::PgLanguage => pg_language_desc(),
        BootstrapCatalogKind::PgOperator => pg_operator_desc(),
        BootstrapCatalogKind::PgDatabase => pg_database_desc(),
        BootstrapCatalogKind::PgAuthId => pg_authid_desc(),
        BootstrapCatalogKind::PgAuthMembers => pg_auth_members_desc(),
        BootstrapCatalogKind::PgCollation => pg_collation_desc(),
        BootstrapCatalogKind::PgTablespace => pg_tablespace_desc(),
        BootstrapCatalogKind::PgAm => pg_am_desc(),
        BootstrapCatalogKind::PgAmop => pg_amop_desc(),
        BootstrapCatalogKind::PgAmproc => pg_amproc_desc(),
        BootstrapCatalogKind::PgAttrdef => pg_attrdef_desc(),
        BootstrapCatalogKind::PgCast => pg_cast_desc(),
        BootstrapCatalogKind::PgConstraint => pg_constraint_desc(),
        BootstrapCatalogKind::PgDepend => pg_depend_desc(),
        BootstrapCatalogKind::PgIndex => pg_index_desc(),
        BootstrapCatalogKind::PgOpclass => pg_opclass_desc(),
        BootstrapCatalogKind::PgOpfamily => pg_opfamily_desc(),
    }
}

pub const fn bootstrap_namespace_oid() -> u32 {
    PG_CATALOG_NAMESPACE_OID
}

pub const CORE_BOOTSTRAP_RELATIONS: [BootstrapCatalogRelation; 22] = [
    BootstrapCatalogRelation {
        oid: PG_NAMESPACE_RELATION_OID,
        name: "pg_namespace",
    },
    BootstrapCatalogRelation {
        oid: PG_TYPE_RELATION_OID,
        name: "pg_type",
    },
    BootstrapCatalogRelation {
        oid: PG_PROC_RELATION_OID,
        name: "pg_proc",
    },
    BootstrapCatalogRelation {
        oid: PG_LANGUAGE_RELATION_OID,
        name: "pg_language",
    },
    BootstrapCatalogRelation {
        oid: PG_OPERATOR_RELATION_OID,
        name: "pg_operator",
    },
    BootstrapCatalogRelation {
        oid: PG_OPFAMILY_RELATION_OID,
        name: "pg_opfamily",
    },
    BootstrapCatalogRelation {
        oid: PG_OPCLASS_RELATION_OID,
        name: "pg_opclass",
    },
    BootstrapCatalogRelation {
        oid: PG_AMOP_RELATION_OID,
        name: "pg_amop",
    },
    BootstrapCatalogRelation {
        oid: PG_AMPROC_RELATION_OID,
        name: "pg_amproc",
    },
    BootstrapCatalogRelation {
        oid: PG_ATTRIBUTE_RELATION_OID,
        name: "pg_attribute",
    },
    BootstrapCatalogRelation {
        oid: PG_CLASS_RELATION_OID,
        name: "pg_class",
    },
    BootstrapCatalogRelation {
        oid: PG_AUTHID_RELATION_OID,
        name: "pg_authid",
    },
    BootstrapCatalogRelation {
        oid: PG_AUTH_MEMBERS_RELATION_OID,
        name: "pg_auth_members",
    },
    BootstrapCatalogRelation {
        oid: PG_COLLATION_RELATION_OID,
        name: "pg_collation",
    },
    BootstrapCatalogRelation {
        oid: PG_DATABASE_RELATION_OID,
        name: "pg_database",
    },
    BootstrapCatalogRelation {
        oid: PG_TABLESPACE_RELATION_OID,
        name: "pg_tablespace",
    },
    BootstrapCatalogRelation {
        oid: PG_AM_RELATION_OID,
        name: "pg_am",
    },
    BootstrapCatalogRelation {
        oid: PG_ATTRDEF_RELATION_OID,
        name: "pg_attrdef",
    },
    BootstrapCatalogRelation {
        oid: PG_CAST_RELATION_OID,
        name: "pg_cast",
    },
    BootstrapCatalogRelation {
        oid: PG_CONSTRAINT_RELATION_OID,
        name: "pg_constraint",
    },
    BootstrapCatalogRelation {
        oid: PG_DEPEND_RELATION_OID,
        name: "pg_depend",
    },
    BootstrapCatalogRelation {
        oid: PG_INDEX_RELATION_OID,
        name: "pg_index",
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_bootstrap_relations_match_expected_oids() {
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[0].oid, PG_NAMESPACE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[1].oid, PG_TYPE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[2].oid, PG_PROC_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[3].oid, PG_LANGUAGE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[4].oid, PG_OPERATOR_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[5].oid, PG_OPFAMILY_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[6].oid, PG_OPCLASS_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[7].oid, PG_AMOP_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[8].oid, PG_AMPROC_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[9].oid, PG_ATTRIBUTE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[10].oid, PG_CLASS_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[11].oid, PG_AUTHID_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[12].oid,
            PG_AUTH_MEMBERS_RELATION_OID
        );
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[13].oid, PG_COLLATION_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[14].oid, PG_DATABASE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[15].oid, PG_TABLESPACE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[16].oid, PG_AM_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[17].oid, PG_ATTRDEF_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[18].oid, PG_CAST_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[19].oid, PG_CONSTRAINT_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[20].oid, PG_DEPEND_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[21].oid, PG_INDEX_RELATION_OID);
    }

    #[test]
    fn core_bootstrap_relation_names_are_stable() {
        let names: Vec<_> = CORE_BOOTSTRAP_RELATIONS
            .iter()
            .map(|rel| rel.name)
            .collect();
        assert_eq!(
            names,
            vec![
                "pg_namespace",
                "pg_type",
                "pg_proc",
                "pg_language",
                "pg_operator",
                "pg_opfamily",
                "pg_opclass",
                "pg_amop",
                "pg_amproc",
                "pg_attribute",
                "pg_class",
                "pg_authid",
                "pg_auth_members",
                "pg_collation",
                "pg_database",
                "pg_tablespace",
                "pg_am",
                "pg_attrdef",
                "pg_cast",
                "pg_constraint",
                "pg_depend",
                "pg_index",
            ]
        );
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
use super::{
    pg_am_desc, pg_attrdef_desc, pg_attribute_desc, pg_auth_members_desc, pg_authid_desc,
    pg_amop_desc, pg_amproc_desc, pg_cast_desc, pg_class_desc, pg_collation_desc,
    pg_constraint_desc, pg_database_desc, pg_depend_desc, pg_index_desc, pg_language_desc,
    pg_namespace_desc, pg_opclass_desc, pg_opfamily_desc, pg_operator_desc, pg_proc_desc,
    pg_tablespace_desc, pg_type_desc,
};
use crate::backend::executor::RelationDesc;
