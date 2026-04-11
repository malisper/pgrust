pub const PG_CATALOG_NAMESPACE_OID: u32 = 11;
pub const PUBLIC_NAMESPACE_OID: u32 = 2200;

pub const PG_TYPE_RELATION_OID: u32 = 1247;
pub const PG_ATTRIBUTE_RELATION_OID: u32 = 1249;
pub const PG_PROC_RELATION_OID: u32 = 1255;
pub const PG_CLASS_RELATION_OID: u32 = 1259;
pub const PG_NAMESPACE_RELATION_OID: u32 = 2615;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapCatalogRelation {
    pub oid: u32,
    pub name: &'static str,
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
}
