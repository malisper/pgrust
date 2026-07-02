use crate::primitive::Oid;

pub const NAMESPACE_RELATION_ID: Oid = 2615;
pub const RELATION_RELATION_ID: Oid = 1259;
pub const DATABASE_RELATION_ID: Oid = 1262;
pub const PROCEDURE_RELATION_ID: Oid = 1255;
pub const TYPE_RELATION_ID: Oid = 1247;
pub const LANGUAGE_RELATION_ID: Oid = 2612;
pub const FOREIGN_SERVER_RELATION_ID: Oid = 1417;
pub const FOREIGN_DATA_WRAPPER_RELATION_ID: Oid = 2328;
pub const TABLE_SPACE_RELATION_ID: Oid = 1213;
pub const AUTH_ID_RELATION_ID: Oid = 1260;
pub const AUTH_ID_OID_INDEX_ID: Oid = 2677;
pub const AUTH_MEM_RELATION_ID: Oid = 1261;
pub const AUTH_MEM_OID_INDEX_ID: Oid = 6303;
pub const ATTRIBUTE_RELATION_ID: Oid = 1249;
pub const INDEX_RELATION_ID: Oid = 2610;
pub const CONSTRAINT_RELATION_ID: Oid = 2606;

pub const PG_CATALOG_NAMESPACE: Oid = 11;
pub const PG_TOAST_NAMESPACE: Oid = 99;
pub const BOOTSTRAP_SUPERUSERID: Oid = 10;
pub const ROLE_PG_DATABASE_OWNER: Oid = 6171;

pub const FirstGenbkiObjectId: Oid = 10000;
pub const FirstUnpinnedObjectId: Oid = 12000;
pub const FirstNormalObjectId: Oid = 16384;

pub const OIDOID: Oid = 26;
pub const BOOLOID: Oid = 16;
pub const BOOL_BTREE_FAM_OID: Oid = 424;
pub const BOOL_HASH_FAM_OID: Oid = 2222;
pub const INT8OID: Oid = 20;
pub const INT4OID: Oid = 23;
pub const INT2OID: Oid = 21;
pub const VOIDOID: Oid = 2278;
pub const INTERNALOID: Oid = 2281;
pub const TEXTOID: Oid = 25;
pub const TEXTARRAYOID: Oid = 1009;
pub const RECORDOID: Oid = 2249;
pub const INT2VECTOROID: Oid = 22;
pub const OIDVECTOROID: Oid = 30;
pub const INT2ARRAYOID: Oid = 1005;
pub const OIDARRAYOID: Oid = 1028;
pub const UNKNOWNOID: Oid = 705;
pub const FLOAT4OID: Oid = 700;
pub const FLOAT8OID: Oid = 701;
pub const BITOID: Oid = 1560;
pub const NUMERICOID: Oid = 1700;

pub const BTREE_AM_OID: Oid = 403;

pub const INDEX_AM_HANDLEROID: Oid = 325;

pub const TABLE_AM_HANDLEROID: Oid = 269;

// `ScanKeyInit` always stamps this into `sk_collation`.
pub const C_COLLATION_OID: Oid = 950;
pub const POSIX_COLLATION_OID: Oid = 951;
pub const DEFAULT_COLLATION_OID: Oid = 100;

pub const RELPERSISTENCE_PERMANENT: u8 = b'p';
pub const RELPERSISTENCE_UNLOGGED: u8 = b'u';
pub const RELPERSISTENCE_TEMP: u8 = b't';
