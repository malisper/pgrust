//! `pg_type` catalog-row vocabulary (`catalog/pg_type.h`).

use types_core::primitive::{Oid, RegProcedure};

use crate::heaptuple::NameData;

/// On-disk layout of a `pg_type` catalog tuple
/// (`catalog/pg_type.h`: `FormData_pg_type`), through `typcollation` — the
/// last fixed-length (non-`CATALOG_VARLEN`) column. `GETSTRUCT` casts a heap
/// tuple's data to this; field order matches PostgreSQL. The trailing
/// variable-length columns (`typdefaultbin`, `typdefault`, `typacl`) are not
/// part of the fixed struct and are accessed via the tuple descriptor, exactly
/// as in C.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FormData_pg_type {
    pub oid: Oid,
    pub typname: NameData,
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
    pub typsubscript: RegProcedure,
    pub typelem: Oid,
    pub typarray: Oid,
    pub typinput: RegProcedure,
    pub typoutput: RegProcedure,
    pub typreceive: RegProcedure,
    pub typsend: RegProcedure,
    pub typmodin: RegProcedure,
    pub typmodout: RegProcedure,
    pub typanalyze: RegProcedure,
    pub typalign: i8,
    pub typstorage: i8,
    pub typnotnull: bool,
    pub typbasetype: Oid,
    pub typtypmod: i32,
    pub typndims: i32,
    pub typcollation: Oid,
}
