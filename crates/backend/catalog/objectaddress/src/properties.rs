// ObjectProperty (objectaddress.c): per-class catalog metadata driving the
// generic rename/namespace/owner paths. attnum 0 = InvalidAttrNumber,
// catcache -1 = none. Rows and values are pinned to the vendored 18.3
// headers.
use cache_syscache::cacheinfo as ci;
use types_core::Oid;
use types_nodes::parsenodes::ObjectType;

pub struct ObjectPropertyType {
    pub class_descr: &'static str,
    pub class_oid: Oid,
    pub oid_index_oid: Oid,
    pub oid_catcache_id: i32,
    pub name_catcache_id: i32,
    pub attnum_oid: i32,
    pub attnum_name: i32,
    pub attnum_namespace: i32,
    pub attnum_owner: i32,
    pub attnum_acl: i32,
    pub objtype: Option<ObjectType>,
    pub is_nsp_name_unique: bool,
}

const fn prop(
    class_descr: &'static str,
    class_oid: Oid,
    oid_index_oid: Oid,
    oid_catcache_id: i32,
    name_catcache_id: i32,
    attnum_name: i32,
    attnum_namespace: i32,
    attnum_owner: i32,
    attnum_acl: i32,
    objtype: Option<ObjectType>,
    is_nsp_name_unique: bool,
) -> ObjectPropertyType {
    ObjectPropertyType {
        class_descr,
        class_oid,
        oid_index_oid,
        oid_catcache_id,
        name_catcache_id,
        attnum_oid: 1,
        attnum_name,
        attnum_namespace,
        attnum_owner,
        attnum_acl,
        objtype,
        is_nsp_name_unique,
    }
}

use ObjectType::*;

#[rustfmt::skip]
static OBJECT_PROPERTY: [ObjectPropertyType; 37] = [
    prop("access method", 2601, 2652, ci::AMOID, ci::AMNAME, 2, 0, 0, 0, Some(OBJECT_ACCESS_METHOD), true),
    prop("access method operator", 2602, 2756, -1, -1, 0, 0, 0, 0, Some(OBJECT_AMOP), false),
    prop("access method procedure", 2603, 2757, -1, -1, 0, 0, 0, 0, Some(OBJECT_AMPROC), false),
    prop("cast", 2605, 2660, -1, -1, 0, 0, 0, 0, Some(OBJECT_CAST), false),
    prop("collation", 3456, 3085, ci::COLLOID, -1, 2, 3, 4, 0, Some(OBJECT_COLLATION), true),
    prop("constraint", 2606, 2667, ci::CONSTROID, -1, 2, 3, 0, 0, None, false),
    prop("conversion", 2607, 2670, ci::CONVOID, ci::CONNAMENSP, 2, 3, 4, 0, Some(OBJECT_CONVERSION), true),
    prop("database", 1262, 2672, ci::DATABASEOID, -1, 2, 0, 3, 18, Some(OBJECT_DATABASE), true),
    prop("default ACL", 826, 828, -1, -1, 0, 0, 0, 0, Some(OBJECT_DEFACL), false),
    prop("extension", 3079, 3080, -1, -1, 2, 0, 3, 0, Some(OBJECT_EXTENSION), true),
    prop("foreign-data wrapper", 2328, 112, ci::FOREIGNDATAWRAPPEROID, ci::FOREIGNDATAWRAPPERNAME, 2, 0, 3, 6, Some(OBJECT_FDW), true),
    prop("foreign server", 1417, 113, ci::FOREIGNSERVEROID, ci::FOREIGNSERVERNAME, 2, 0, 3, 7, Some(OBJECT_FOREIGN_SERVER), true),
    prop("function", 1255, 2690, ci::PROCOID, -1, 2, 3, 4, 30, Some(OBJECT_FUNCTION), false),
    prop("language", 2612, 2682, ci::LANGOID, ci::LANGNAME, 2, 0, 3, 9, Some(OBJECT_LANGUAGE), true),
    prop("large object metadata", 2995, 2996, -1, -1, 0, 0, 2, 3, Some(OBJECT_LARGEOBJECT), false),
    prop("operator class", 2616, 2687, ci::CLAOID, -1, 3, 4, 5, 0, Some(OBJECT_OPCLASS), true),
    prop("operator", 2617, 2688, ci::OPEROID, -1, 2, 3, 4, 0, Some(OBJECT_OPERATOR), false),
    prop("operator family", 2753, 2755, ci::OPFAMILYOID, -1, 3, 4, 5, 0, Some(OBJECT_OPFAMILY), true),
    prop("role", 1260, 2677, ci::AUTHOID, ci::AUTHNAME, 2, 0, 0, 0, Some(OBJECT_ROLE), true),
    prop("role membership", 1261, 6303, -1, -1, 0, 0, 4, 0, None, true),
    prop("rule", 2618, 2692, -1, -1, 2, 0, 0, 0, Some(OBJECT_RULE), false),
    prop("schema", 2615, 2685, ci::NAMESPACEOID, ci::NAMESPACENAME, 2, 0, 3, 4, Some(OBJECT_SCHEMA), true),
    prop("relation", 1259, 2662, ci::RELOID, ci::RELNAMENSP, 2, 3, 6, 32, Some(OBJECT_TABLE), true),
    prop("tablespace", 1213, 2697, ci::TABLESPACEOID, -1, 2, 0, 3, 4, Some(OBJECT_TABLESPACE), true),
    prop("transform", 3576, 3574, ci::TRFOID, -1, 0, 0, 0, 0, Some(OBJECT_TRANSFORM), false),
    prop("trigger", 2620, 2702, -1, -1, 4, 0, 0, 0, Some(OBJECT_TRIGGER), false),
    prop("policy", 3256, 3257, -1, -1, 2, 0, 0, 0, Some(OBJECT_POLICY), false),
    prop("event trigger", 3466, 3468, ci::EVENTTRIGGEROID, ci::EVENTTRIGGERNAME, 2, 0, 4, 0, Some(OBJECT_EVENT_TRIGGER), true),
    prop("text search configuration", 3602, 3712, ci::TSCONFIGOID, ci::TSCONFIGNAMENSP, 2, 3, 4, 0, Some(OBJECT_TSCONFIGURATION), true),
    prop("text search dictionary", 3600, 3605, ci::TSDICTOID, ci::TSDICTNAMENSP, 2, 3, 4, 0, Some(OBJECT_TSDICTIONARY), true),
    prop("text search parser", 3601, 3607, ci::TSPARSEROID, ci::TSPARSERNAMENSP, 2, 3, 0, 0, Some(OBJECT_TSPARSER), true),
    prop("text search template", 3764, 3767, ci::TSTEMPLATEOID, ci::TSTEMPLATENAMENSP, 2, 3, 0, 0, Some(OBJECT_TSTEMPLATE), true),
    prop("type", 1247, 2703, ci::TYPEOID, ci::TYPENAMENSP, 2, 3, 4, 32, Some(OBJECT_TYPE), true),
    prop("publication", 6104, 6110, ci::PUBLICATIONOID, ci::PUBLICATIONNAME, 2, 0, 3, 0, Some(OBJECT_PUBLICATION), true),
    prop("subscription", 6100, 6114, ci::SUBSCRIPTIONOID, ci::SUBSCRIPTIONNAME, 4, 0, 5, 0, Some(OBJECT_SUBSCRIPTION), true),
    prop("extended statistics", 3381, 3380, ci::STATEXTOID, ci::STATEXTNAMENSP, 3, 4, 5, 0, Some(OBJECT_STATISTIC_EXT), true),
    prop("user mapping", 1418, 174, ci::USERMAPPINGOID, -1, 0, 0, 0, 0, Some(OBJECT_USER_MAPPING), false),
];

pub fn get_object_property_data(class_id: Oid) -> &'static ObjectPropertyType {
    OBJECT_PROPERTY
        .iter()
        .find(|p| p.class_oid == class_id)
        .unwrap_or_else(|| panic!("unrecognized class ID: {class_id}"))
}

pub fn is_objectclass_supported(class_id: Oid) -> bool {
    OBJECT_PROPERTY.iter().any(|p| p.class_oid == class_id)
}

pub fn get_object_namensp_unique(class_id: Oid) -> bool {
    get_object_property_data(class_id).is_nsp_name_unique
}

pub fn get_object_catcache_oid(class_id: Oid) -> i32 {
    get_object_property_data(class_id).oid_catcache_id
}

pub fn get_object_catcache_name(class_id: Oid) -> i32 {
    get_object_property_data(class_id).name_catcache_id
}

pub fn get_object_oid_index(class_id: Oid) -> Oid {
    get_object_property_data(class_id).oid_index_oid
}

pub fn get_object_attnum_oid(class_id: Oid) -> i32 {
    get_object_property_data(class_id).attnum_oid
}

pub fn get_object_attnum_name(class_id: Oid) -> i32 {
    get_object_property_data(class_id).attnum_name
}

pub fn get_object_attnum_namespace(class_id: Oid) -> i32 {
    get_object_property_data(class_id).attnum_namespace
}

pub fn get_object_attnum_owner(class_id: Oid) -> i32 {
    get_object_property_data(class_id).attnum_owner
}

pub fn get_object_attnum_acl(class_id: Oid) -> i32 {
    get_object_property_data(class_id).attnum_acl
}

pub fn get_object_class_descr(class_id: Oid) -> &'static str {
    get_object_property_data(class_id).class_descr
}

// get_object_type (objectaddress.c): for the OBJECT_TABLE property class, dig
// out the real relation kind so callers emit precise error nouns.
pub fn get_object_type(class_id: Oid, object_id: Oid) -> types_error::PgResult<ObjectType> {
    match get_object_property_data(class_id).objtype {
        Some(ObjectType::OBJECT_TABLE) => {
            let relkind = lsyscache::relation::get_rel_relkind(object_id)? as u8;
            Ok(get_relkind_objtype(relkind))
        }
        Some(t) => Ok(t),
        None => panic!("unsupported object type: {class_id} for object {object_id}"),
    }
}

// get_relkind_objtype (objectaddress.c): relkind -> ObjectType; default TABLE.
fn get_relkind_objtype(relkind: u8) -> ObjectType {
    use types_rel::{
        RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX,
        RELKIND_SEQUENCE, RELKIND_VIEW,
    };
    match relkind {
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => ObjectType::OBJECT_INDEX,
        RELKIND_SEQUENCE => ObjectType::OBJECT_SEQUENCE,
        RELKIND_VIEW => ObjectType::OBJECT_VIEW,
        RELKIND_MATVIEW => ObjectType::OBJECT_MATVIEW,
        RELKIND_FOREIGN_TABLE => ObjectType::OBJECT_FOREIGN_TABLE,
        _ => ObjectType::OBJECT_TABLE,
    }
}
