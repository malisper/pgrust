//! `pg_type` catalog vocabulary (`catalog/pg_type.h`, PostgreSQL 18.3) used by
//! the `backend-catalog-pg-type` owner and the `commands/typecmds.c` consumer.
//!
//! The on-disk fixed-part struct (`FormData_pg_type`) lives in
//! `types_tuple::pg_type`; this module adds the catalog relation/index OIDs,
//! the attribute numbers (`Anum_pg_type_*`), the `typtype`/`typcategory`
//! single-char codes pg_type.c writes, and the value-layer row/field carriers
//! that cross the catalog-tuple seams (`TypeFormFields`, `TypeCreateParams`,
//! `TypeAttrUpdate`, `PgTypeInsertRow`).

extern crate alloc;

use alloc::string::String;

use ::types_core::primitive::{InvalidOid, Oid};

/* ==========================================================================
 * Catalog relation + index OIDs (pg_type.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `TypeRelationId` — `pg_type` (OID 1247).
pub const TypeRelationId: Oid = 1247;
/// `TypeOidIndexId` — `pg_type_oid_index` (OID 2703).
pub const TypeOidIndexId: Oid = 2703;
/// `TypeNameNspIndexId` — `pg_type_typname_nsp_index` (OID 2704).
pub const TypeNameNspIndexId: Oid = 2704;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_type; 1-based).
 * ======================================================================== */

pub const Anum_pg_type_oid: i16 = 1;
pub const Anum_pg_type_typname: i16 = 2;
pub const Anum_pg_type_typnamespace: i16 = 3;
pub const Anum_pg_type_typowner: i16 = 4;
pub const Anum_pg_type_typlen: i16 = 5;
pub const Anum_pg_type_typbyval: i16 = 6;
pub const Anum_pg_type_typtype: i16 = 7;
pub const Anum_pg_type_typcategory: i16 = 8;
pub const Anum_pg_type_typispreferred: i16 = 9;
pub const Anum_pg_type_typisdefined: i16 = 10;
pub const Anum_pg_type_typdelim: i16 = 11;
pub const Anum_pg_type_typrelid: i16 = 12;
pub const Anum_pg_type_typsubscript: i16 = 13;
pub const Anum_pg_type_typelem: i16 = 14;
pub const Anum_pg_type_typarray: i16 = 15;
pub const Anum_pg_type_typinput: i16 = 16;
pub const Anum_pg_type_typoutput: i16 = 17;
pub const Anum_pg_type_typreceive: i16 = 18;
pub const Anum_pg_type_typsend: i16 = 19;
pub const Anum_pg_type_typmodin: i16 = 20;
pub const Anum_pg_type_typmodout: i16 = 21;
pub const Anum_pg_type_typanalyze: i16 = 22;
pub const Anum_pg_type_typalign: i16 = 23;
pub const Anum_pg_type_typstorage: i16 = 24;
pub const Anum_pg_type_typnotnull: i16 = 25;
pub const Anum_pg_type_typbasetype: i16 = 26;
pub const Anum_pg_type_typtypmod: i16 = 27;
pub const Anum_pg_type_typndims: i16 = 28;
pub const Anum_pg_type_typcollation: i16 = 29;
pub const Anum_pg_type_typdefaultbin: i16 = 30;
pub const Anum_pg_type_typdefault: i16 = 31;
pub const Anum_pg_type_typacl: i16 = 32;

/// `Natts_pg_type` — number of columns of `pg_type`.
pub const Natts_pg_type: usize = 32;

/* ==========================================================================
 * typtype / typcategory codes (pg_type.h TYPTYPE_ / TYPCATEGORY_ macros).
 * ======================================================================== */

/// `TYPTYPE_BASE` — base type.
pub const TYPTYPE_BASE: i8 = b'b' as i8;
/// `TYPTYPE_COMPOSITE` — composite type (table rowtype).
pub const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
/// `TYPTYPE_DOMAIN` — domain over another type.
pub const TYPTYPE_DOMAIN: i8 = b'd' as i8;
/// `TYPTYPE_ENUM` — enum type.
pub const TYPTYPE_ENUM: i8 = b'e' as i8;
/// `TYPTYPE_MULTIRANGE` — multirange type.
pub const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;
/// `TYPTYPE_PSEUDO` — pseudo-type.
pub const TYPTYPE_PSEUDO: i8 = b'p' as i8;
/// `TYPTYPE_RANGE` — range type.
pub const TYPTYPE_RANGE: i8 = b'r' as i8;

/// `TYPCATEGORY_PSEUDOTYPE` — the category of pseudo-types.
pub const TYPCATEGORY_PSEUDOTYPE: i8 = b'P' as i8;

/// `F_SHELL_IN` (fmgroids.h) — OID of the `shell_in` builtin (pg_proc.dat 2398).
pub const F_SHELL_IN: Oid = 2398;
/// `F_SHELL_OUT` (fmgroids.h) — OID of the `shell_out` builtin (pg_proc.dat 2399).
pub const F_SHELL_OUT: Oid = 2399;

/// `DEFAULT_TYPDELIM` (commands/typecmds.h) — default array element delimiter.
pub const DEFAULT_TYPDELIM: i8 = b',' as i8;

/* ==========================================================================
 * Value-layer carriers consumed by typecmds.c and the catalog-tuple seams.
 * ======================================================================== */

/// The `Form_pg_type` fixed fields the in-crate decision logic of `typecmds.c`
/// reads off a fetched row (`(Form_pg_type) GETSTRUCT(tup)`). Field types/order
/// verified against `FormData_pg_type` (catalog/pg_type.h).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeFormFields {
    pub oid: Oid,
    pub typname: String,
    pub typnamespace: Oid,
    pub typowner: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typtype: i8,
    pub typcategory: i8,
    pub typispreferred: bool,
    pub typisdefined: bool,
    pub typdelim: i8,
    pub typrelid: Oid,
    pub typsubscript: Oid,
    pub typelem: Oid,
    pub typarray: Oid,
    pub typinput: Oid,
    pub typoutput: Oid,
    pub typreceive: Oid,
    pub typsend: Oid,
    pub typmodin: Oid,
    pub typmodout: Oid,
    pub typanalyze: Oid,
    pub typalign: i8,
    pub typstorage: i8,
    pub typnotnull: bool,
    pub typbasetype: Oid,
    pub typtypmod: i32,
    pub typndims: i32,
    pub typcollation: Oid,
}

/// The full set of `TypeCreate()` arguments (`catalog/pg_type.c`). One owned
/// bundle instead of the 32-positional C parameter list; field types verified
/// against the `TypeCreate` prototype.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeCreateParams {
    pub new_type_oid: Oid,
    pub type_name: String,
    pub type_namespace: Oid,
    pub relation_oid: Oid,
    pub relation_kind: i8,
    pub owner_id: Oid,
    pub internal_size: i16,
    pub type_type: i8,
    pub type_category: i8,
    pub type_preferred: bool,
    pub type_delim: i8,
    pub input_procedure: Oid,
    pub output_procedure: Oid,
    pub receive_procedure: Oid,
    pub send_procedure: Oid,
    pub typmodin_procedure: Oid,
    pub typmodout_procedure: Oid,
    pub analyze_procedure: Oid,
    pub subscript_procedure: Oid,
    pub element_type: Oid,
    pub is_implicit_array: bool,
    pub array_type: Oid,
    pub base_type: Oid,
    /// `defaultTypeValue` (human-readable rep), `None` for NULL.
    pub default_type_value: Option<String>,
    /// `defaultTypeBin` (cooked nodeToString rep), `None` for NULL.
    pub default_type_bin: Option<String>,
    pub passed_by_value: bool,
    pub alignment: i8,
    pub storage: i8,
    pub type_mod: i32,
    pub typ_ndims: i32,
    pub type_not_null: bool,
    pub type_collation: Oid,
}

/// Which `Anum_pg_type_*` columns `AlterTypeRecurse` (typecmds.c) overwrites
/// on a held tuple. Each `update_*` gate corresponds to a C `replaces[]`
/// entry; the matching `*_oid`/`storage` carries the new value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TypeAttrUpdate {
    pub update_storage: bool,
    pub update_receive: bool,
    pub update_send: bool,
    pub update_typmodin: bool,
    pub update_typmodout: bool,
    pub update_analyze: bool,
    pub update_subscript: bool,
    pub storage: i8,
    pub receive_oid: Oid,
    pub send_oid: Oid,
    pub typmodin_oid: Oid,
    pub typmodout_oid: Oid,
    pub analyze_oid: Oid,
    pub subscript_oid: Oid,
}

/// One `pg_type` row to be formed and inserted/updated by the catalog-tuple
/// owner (`catalog/indexing.c`): the fixed-part [`TypeFormFields`] plus the
/// three trailing `CATALOG_VARLEN` columns. `None` for any of the varlen
/// columns is the C `nulls[Anum_pg_type_* - 1] = true`.
#[derive(Clone, Debug)]
pub struct PgTypeInsertRow {
    /// The fixed-width columns (`oid` is the row OID the owner assigned).
    pub fields: TypeFormFields,
    /// `typdefaultbin` — the cooked default expression `nodeToString` text.
    pub typdefaultbin: Option<String>,
    /// `typdefault` — the human-readable default value text.
    pub typdefault: Option<String>,
    /// `typacl` — the `aclitem[]` ACL array (`Acl *`), as its full on-disk
    /// varlena image (`None` ≡ SQL NULL).
    pub typacl: Option<alloc::vec::Vec<u8>>,
}

/// `setconfig`-style decode of the `Acl *` typacl, plus the `defaultTypeBin`
/// text, that `GenerateTypeDependencies` may need to re-extract from the tuple
/// when the caller passed `NULL`. The owner reads these off the fetched row.
#[derive(Clone, Debug, Default)]
pub struct TypeTupleExtras {
    /// `heap_getattr(typeTuple, Anum_pg_type_typdefaultbin)` text, or `None`
    /// when the column is SQL NULL.
    pub typdefaultbin: Option<String>,
    /// `DatumGetAclPCopy(heap_getattr(typeTuple, Anum_pg_type_typacl))`, or
    /// `None` when the column is SQL NULL. Full on-disk varlena image.
    pub typacl: Option<alloc::vec::Vec<u8>>,
}

/// `TypeCreateParams` and the helpers compute the same `Vec`-free fixed
/// columns; this returns the `oid`-less [`TypeFormFields`] for a fresh shell
/// type's dummy values, matching `TypeShellMake`'s int4-like representation.
impl TypeFormFields {
    /// `TypeShellMake`'s dummy `values[]` population (pg_type.c:94-122), minus
    /// the still-unassigned `oid` (set to `InvalidOid` here; the owner stamps
    /// the real OID).
    pub fn shell(typname: String, typnamespace: Oid, owner_id: Oid) -> Self {
        TypeFormFields {
            oid: InvalidOid,
            typname,
            typnamespace,
            typowner: owner_id,
            typlen: core::mem::size_of::<i32>() as i16,
            typbyval: true,
            typtype: TYPTYPE_PSEUDO,
            typcategory: TYPCATEGORY_PSEUDOTYPE,
            typispreferred: false,
            typisdefined: false,
            typdelim: DEFAULT_TYPDELIM,
            typrelid: InvalidOid,
            typsubscript: InvalidOid,
            typelem: InvalidOid,
            typarray: InvalidOid,
            typinput: F_SHELL_IN,
            typoutput: F_SHELL_OUT,
            typreceive: InvalidOid,
            typsend: InvalidOid,
            typmodin: InvalidOid,
            typmodout: InvalidOid,
            typanalyze: InvalidOid,
            typalign: types_tuple::heaptuple::TYPALIGN_INT,
            typstorage: types_tuple::heaptuple::TYPSTORAGE_PLAIN,
            typnotnull: false,
            typbasetype: InvalidOid,
            typtypmod: -1,
            typndims: 0,
            typcollation: InvalidOid,
        }
    }
}

/// Build the full fixed-part [`TypeFormFields`] for a `TypeCreate` row from its
/// params (pg_type.c:352-380), minus the `oid` (the owner assigns it). All the
/// pure `values[]` population that does not touch the open relation lives here.
pub fn type_create_fields(params: &TypeCreateParams) -> TypeFormFields {
    TypeFormFields {
        oid: InvalidOid,
        typname: params.type_name.clone(),
        typnamespace: params.type_namespace,
        typowner: params.owner_id,
        typlen: params.internal_size,
        typbyval: params.passed_by_value,
        typtype: params.type_type,
        typcategory: params.type_category,
        typispreferred: params.type_preferred,
        typisdefined: true,
        typdelim: params.type_delim,
        typrelid: params.relation_oid,
        typsubscript: params.subscript_procedure,
        typelem: params.element_type,
        typarray: params.array_type,
        typinput: params.input_procedure,
        typoutput: params.output_procedure,
        typreceive: params.receive_procedure,
        typsend: params.send_procedure,
        typmodin: params.typmodin_procedure,
        typmodout: params.typmodout_procedure,
        typanalyze: params.analyze_procedure,
        typalign: params.alignment,
        typstorage: params.storage,
        typnotnull: params.type_not_null,
        typbasetype: params.base_type,
        typtypmod: params.type_mod,
        typndims: params.typ_ndims,
        typcollation: params.type_collation,
    }
}
