//! `pg_enum` catalog row layout and constants (`catalog/pg_enum.h`,
//! PostgreSQL 18.3), trimmed to what the `backend-catalog-pg-enum` port reads.

use ::types_core::primitive::Oid;
use ::types_core::TransactionId;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_enum.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `EnumRelationId` — `pg_enum` (OID 3501).
pub const EnumRelationId: Oid = 3501;
/// `EnumOidIndexId` — `pg_enum_oid_index` (OID 3502).
pub const EnumOidIndexId: Oid = 3502;
/// `EnumTypIdLabelIndexId` — `pg_enum_typid_label_index` (OID 3503).
pub const EnumTypIdLabelIndexId: Oid = 3503;
/// `EnumTypIdSortOrderIndexId` — `pg_enum_typid_sortorder_index` (OID 3534).
pub const EnumTypIdSortOrderIndexId: Oid = 3534;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_enum).
 * ======================================================================== */

pub const Anum_pg_enum_oid: i16 = 1;
pub const Anum_pg_enum_enumtypid: i16 = 2;
pub const Anum_pg_enum_enumsortorder: i16 = 3;
pub const Anum_pg_enum_enumlabel: i16 = 4;

/// `Natts_pg_enum` — number of columns.
pub const Natts_pg_enum: usize = 4;

/* ==========================================================================
 * Row carriers.
 * ======================================================================== */

/// The fixed-width scalar columns of one scanned `pg_enum` row
/// (`(Form_pg_enum) GETSTRUCT(tup)`). `enumlabel` is the 64-byte `NameData`
/// image. All columns are non-null fixed-length.
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_enum {
    pub oid: Oid,
    pub enumtypid: Oid,
    pub enumsortorder: f32,
    pub enumlabel: [u8; 64],
}

/// One scanned `pg_enum` tuple as the `enum.c` ADT support functions consume
/// it: the `(Form_pg_enum) GETSTRUCT(tup)` scalar columns enum.c reads (`oid`,
/// `enumtypid`, `enumlabel`) plus the tuple-header facts `check_safe_enum_use`
/// (enum.c:60) needs — `xmin_committed` (`HeapTupleHeaderXminCommitted`) and
/// `xmin` (`HeapTupleHeaderGetXmin`, the frozen-aware effective xmin). The
/// producer (syscache projection / `pg_enum` ordered scan) drops the cache
/// reference / closes the scan before returning, so this is a self-contained
/// owned copy. `enumlabel` is the 64-byte `NameData` image.
#[derive(Clone, Copy, Debug)]
pub struct EnumTupleData {
    pub oid: Oid,
    pub enumtypid: Oid,
    pub enumlabel: [u8; 64],
    /// `HeapTupleHeaderXminCommitted(tup->t_data)` — the committed hint bit.
    pub xmin_committed: bool,
    /// `HeapTupleHeaderGetXmin(tup->t_data)` — the effective (frozen-aware) xmin.
    pub xmin: TransactionId,
}

/// The values `EnumValuesCreate` / `AddEnumLabel` build for `heap_form_tuple`
/// + `CatalogTupleInsert`. The owner supplies the freshly-allocated `oid`
/// (caller computes it via `GetNewOidWithIndex` and passes it in, because the
/// even/odd OID selection is logic that lives in the port, not the heapam
/// owner). All columns are non-null fixed-length.
#[derive(Clone, Copy, Debug)]
pub struct PgEnumInsertRow {
    pub oid: Oid,
    pub enumtypid: Oid,
    pub enumsortorder: f32,
    pub enumlabel: [u8; 64],
}
