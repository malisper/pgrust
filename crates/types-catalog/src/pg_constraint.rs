//! `pg_constraint` catalog row layout and constants (`catalog/pg_constraint.h`,
//! PostgreSQL 18.3), trimmed to what the `backend-catalog-pg-constraint` port
//! reads.

extern crate alloc;

use alloc::vec::Vec;
use types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_constraint.h DECLARE_*).
 * ======================================================================== */

/// `ConstraintOidIndexId` — `pg_constraint_oid_index` (OID 2667).
pub const ConstraintOidIndexId: Oid = 2667;
/// `ConstraintNameNspIndexId` — `pg_constraint_conname_nsp_index` (OID 2664).
pub const ConstraintNameNspIndexId: Oid = 2664;
/// `ConstraintRelidTypidNameIndexId` —
/// `pg_constraint_conrelid_contypid_conname_index` (OID 2665).
pub const ConstraintRelidTypidNameIndexId: Oid = 2665;
/// `ConstraintTypidIndexId` — `pg_constraint_contypid_index` (OID 2666).
pub const ConstraintTypidIndexId: Oid = 2666;

/* ==========================================================================
 * Constraint type codes (pg_constraint.h).
 * ======================================================================== */

/// `CONSTRAINT_CHECK` — `'c'`.
pub const CONSTRAINT_CHECK: i8 = b'c' as i8;
/// `CONSTRAINT_FOREIGN` — `'f'`.
pub const CONSTRAINT_FOREIGN: i8 = b'f' as i8;
/// `CONSTRAINT_NOTNULL` — `'n'`.
pub const CONSTRAINT_NOTNULL: i8 = b'n' as i8;
/// `CONSTRAINT_PRIMARY` — `'p'`.
pub const CONSTRAINT_PRIMARY: i8 = b'p' as i8;
/// `CONSTRAINT_UNIQUE` — `'u'`.
pub const CONSTRAINT_UNIQUE: i8 = b'u' as i8;
/// `CONSTRAINT_EXCLUSION` — `'x'`.
pub const CONSTRAINT_EXCLUSION: i8 = b'x' as i8;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_constraint).
 * ======================================================================== */

pub const Anum_pg_constraint_oid: i16 = 1;
pub const Anum_pg_constraint_conname: i16 = 2;
pub const Anum_pg_constraint_connamespace: i16 = 3;
pub const Anum_pg_constraint_contype: i16 = 4;
pub const Anum_pg_constraint_condeferrable: i16 = 5;
pub const Anum_pg_constraint_condeferred: i16 = 6;
pub const Anum_pg_constraint_conenforced: i16 = 7;
pub const Anum_pg_constraint_convalidated: i16 = 8;
pub const Anum_pg_constraint_conrelid: i16 = 9;
pub const Anum_pg_constraint_contypid: i16 = 10;
pub const Anum_pg_constraint_conindid: i16 = 11;
pub const Anum_pg_constraint_conparentid: i16 = 12;
pub const Anum_pg_constraint_confrelid: i16 = 13;
pub const Anum_pg_constraint_confupdtype: i16 = 14;
pub const Anum_pg_constraint_confdeltype: i16 = 15;
pub const Anum_pg_constraint_confmatchtype: i16 = 16;
pub const Anum_pg_constraint_conislocal: i16 = 17;
pub const Anum_pg_constraint_coninhcount: i16 = 18;
pub const Anum_pg_constraint_connoinherit: i16 = 19;
pub const Anum_pg_constraint_conperiod: i16 = 20;
pub const Anum_pg_constraint_conkey: i16 = 21;
pub const Anum_pg_constraint_confkey: i16 = 22;
pub const Anum_pg_constraint_conpfeqop: i16 = 23;
pub const Anum_pg_constraint_conppeqop: i16 = 24;
pub const Anum_pg_constraint_conffeqop: i16 = 25;
pub const Anum_pg_constraint_confdelsetcols: i16 = 26;
pub const Anum_pg_constraint_conexclop: i16 = 27;
pub const Anum_pg_constraint_conbin: i16 = 28;

/// `Natts_pg_constraint` — number of columns.
pub const Natts_pg_constraint: usize = 28;

/* ==========================================================================
 * PERIOD intersect operator OIDs (pg_operator.dat).
 * ======================================================================== */

/// `OID_RANGE_INTERSECT_RANGE_OP` — `anyrange * anyrange` (OID 3900).
pub const OID_RANGE_INTERSECT_RANGE_OP: Oid = 3900;
/// `OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP` — `anymultirange * anymultirange`
/// (OID 4394).
pub const OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP: Oid = 4394;

/* ==========================================================================
 * ConstraintCategory (pg_constraint.h) — the relation/domain/type discriminant
 * `ConstraintNameIsUsed` / `RenameConstraintById` switch on.
 * ======================================================================== */

/// `ConstraintCategory` — whether a constraint belongs to a relation, a domain,
/// or a type. (`CONSTRAINT_RELATION` / `CONSTRAINT_DOMAIN` / `CONSTRAINT_ASSERTION`
/// in pg_constraint.h.)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConstraintCategory {
    /// `CONSTRAINT_RELATION`.
    Relation,
    /// `CONSTRAINT_DOMAIN`.
    Domain,
    /// `CONSTRAINT_ASSERTION`.
    Type,
}

/// `(Form_pg_constraint) GETSTRUCT(tup)` — the fixed-width scalar columns of a
/// `pg_constraint` row (everything up through `conperiod`; the trailing
/// variable-length array columns are read separately). `conname` is the raw
/// `NameData` bytes.
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_constraint {
    pub oid: Oid,
    pub conname: [u8; 64],
    pub connamespace: Oid,
    pub contype: i8,
    pub condeferrable: bool,
    pub condeferred: bool,
    pub conenforced: bool,
    pub convalidated: bool,
    pub conrelid: Oid,
    pub contypid: Oid,
    pub conindid: Oid,
    pub conparentid: Oid,
    pub confrelid: Oid,
    pub confupdtype: i8,
    pub confdeltype: i8,
    pub confmatchtype: i8,
    pub conislocal: bool,
    pub coninhcount: i16,
    pub connoinherit: bool,
    pub conperiod: bool,
}

impl FormData_pg_constraint {
    /// `NameStr(conname)` — the constraint name as a `&str` (read up to the NUL).
    pub fn conname_str(&self) -> &str {
        let end = self
            .conname
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.conname.len());
        core::str::from_utf8(&self.conname[..end]).unwrap_or("")
    }
}

/// A 1-D `int16[]` (smallint) array column read from a `pg_constraint` tuple
/// (`conkey` / `confkey` / `confdelsetcols`) — the validated `ARR_*` fields the
/// port checks (mirror of src-idiomatic `ConKeyArray`). `data` is the flattened
/// element data (`ARR_DATA_PTR`).
#[derive(Clone, Debug)]
pub struct ConKeyArray {
    /// `ARR_NDIM`.
    pub ndim: i32,
    /// `ARR_HASNULL`.
    pub hasnull: bool,
    /// `ARR_ELEMTYPE`.
    pub elemtype: Oid,
    /// `ARR_DIMS(arr)[0]`.
    pub dim0: i32,
    /// `ARR_DATA_PTR` reinterpreted as `int16[]`.
    pub data: Vec<i16>,
}

/// A 1-D `Oid[]` array column read from a `pg_constraint` tuple
/// (`conpfeqop` / `conppeqop` / `conffeqop`).
#[derive(Clone, Debug)]
pub struct OidArray {
    /// `ARR_NDIM`.
    pub ndim: i32,
    /// `ARR_HASNULL`.
    pub hasnull: bool,
    /// `ARR_ELEMTYPE`.
    pub elemtype: Oid,
    /// `ARR_DIMS(arr)[0]`.
    pub dim0: i32,
    /// `ARR_DATA_PTR` reinterpreted as `Oid[]`.
    pub data: Vec<Oid>,
}

/// The array columns `DeconstructFkConstraintRow` reads from a FK tuple.
/// `confdelsetcols` is `None` when the column is SQL NULL.
#[derive(Clone, Debug)]
pub struct FkArrayProjection {
    pub conkey: ConKeyArray,
    pub confkey: ConKeyArray,
    pub conpfeqop: OidArray,
    pub conppeqop: OidArray,
    pub conffeqop: OidArray,
    pub confdelsetcols: Option<ConKeyArray>,
}

/// The complete set of `pg_constraint` column values `CreateConstraintEntry`
/// hands to the indexing owner to form + insert (mirror of src-idiomatic
/// `PgConstraintRow`). `conkey`/`confkey`/`confdelsetcols` are the smallint
/// array columns; `conpfeqop`/`conppeqop`/`conffeqop`/`conexclop` are the Oid
/// array columns; `None` means a SQL NULL column. `conbin` is the
/// `pg_node_tree` text (NULL when no CHECK expression).
#[derive(Clone, Debug)]
pub struct PgConstraintInsertRow {
    pub conname: [u8; 64],
    pub connamespace: Oid,
    pub contype: i8,
    pub condeferrable: bool,
    pub condeferred: bool,
    pub conenforced: bool,
    pub convalidated: bool,
    pub conrelid: Oid,
    pub contypid: Oid,
    pub conindid: Oid,
    pub conparentid: Oid,
    pub confrelid: Oid,
    pub confupdtype: i8,
    pub confdeltype: i8,
    pub confmatchtype: i8,
    pub conislocal: bool,
    pub coninhcount: i16,
    pub connoinherit: bool,
    pub conperiod: bool,
    pub conkey: Option<Vec<i16>>,
    pub confkey: Option<Vec<i16>>,
    pub conpfeqop: Option<Vec<Oid>>,
    pub conppeqop: Option<Vec<Oid>>,
    pub conffeqop: Option<Vec<Oid>>,
    pub confdelsetcols: Option<Vec<i16>>,
    pub conexclop: Option<Vec<Oid>>,
    pub conbin: Option<alloc::string::String>,
}

/// The fields `CatalogTupleUpdate` needs to write back for the in-place
/// mutators (`AdjustNotNullInheritance` / `RenameConstraintById` /
/// `AlterConstraintNamespaces` / `ConstraintSetParentConstraint` /
/// `AlterDomainValidateConstraint`): the columns those paths scribble on a
/// copied tuple before re-storing it. The owner re-forms the tuple at `tid`
/// from the existing row with these fields overwritten.
#[derive(Clone, Debug)]
pub struct ConstraintFieldUpdate {
    pub conname: [u8; 64],
    pub connamespace: Oid,
    pub conislocal: bool,
    pub coninhcount: i16,
    pub conparentid: Oid,
    /// `convalidated` — flipped to `true` by `AlterDomainValidateConstraint`;
    /// the other mutator sites carry through the row's existing value.
    pub convalidated: bool,
    /// `connoinherit` — set `true` by `MergeWithExistingConstraint` when merging
    /// a NO INHERIT child into the row; the other mutator sites carry through the
    /// row's existing value.
    pub connoinherit: bool,
    /// `conenforced` — flipped to `true` by `MergeWithExistingConstraint` when an
    /// enforced child constraint is merged into a not-enforced parent; the other
    /// mutator sites carry through the row's existing value.
    pub conenforced: bool,
    /// `condeferrable` — set by `AlterConstrUpdateConstraintEntry` for ALTER
    /// CONSTRAINT ... DEFERRABLE; the other mutator sites carry through the
    /// row's existing value.
    pub condeferrable: bool,
    /// `condeferred` — set by `AlterConstrUpdateConstraintEntry` for ALTER
    /// CONSTRAINT ... INITIALLY DEFERRED; the other mutator sites carry through
    /// the row's existing value.
    pub condeferred: bool,
}

/// A `SearchSysCache1(CONSTROID)` projection: the scalar `Form_pg_constraint`
/// fields plus the `conkey` array column (the syscache form-search seam result
/// used by the conparentid root walk and the FK-row loader).
#[derive(Clone, Debug)]
pub struct ConstraintFormCopy {
    /// The fixed-width scalar columns.
    pub form: FormData_pg_constraint,
    /// `conkey` (1-D smallint), `None` when SQL NULL.
    pub conkey: Option<ConKeyArray>,
    /// `tup->t_self` — the heap TID, for the `CatalogTuple{Update,Delete}`
    /// legs (`RemoveConstraintById` / `RenameConstraintById` /
    /// `ConstraintSetParentConstraint` / `AdjustNotNullInheritance`).
    pub tid: types_tuple::heaptuple::ItemPointerData,
}

/// The deformed `pg_constraint` row `pg_get_constraintdef_worker`
/// (ruleutils.c 2193-2612) reads: the fixed-width scalar form plus the
/// by-reference array / `pg_node_tree` columns the switch arms detoast with
/// `SysCacheGetAttr*`. Each optional array/text is `None` when the column is
/// SQL NULL (matching the C `SysCacheGetAttr(..., &isnull)` reads; the
/// `*NotNull` reads never produce `None` and the caller treats absence as the
/// C `elog` would).
#[derive(Clone, Debug)]
pub struct PgConstraintDefInfo {
    /// `(Form_pg_constraint) GETSTRUCT(tup)` — the fixed-width scalar columns.
    pub form: FormData_pg_constraint,
    /// `conkey` (1-D smallint, FK/PK/UNIQUE referencing columns), `None` when
    /// SQL NULL.
    pub conkey: Option<ConKeyArray>,
    /// `confkey` (1-D smallint, FK referenced columns), `None` when SQL NULL.
    pub confkey: Option<ConKeyArray>,
    /// `confdelsetcols` (1-D smallint, FK ON DELETE SET columns), `None` when
    /// SQL NULL.
    pub confdelsetcols: Option<ConKeyArray>,
    /// `conexclop` (1-D oid, EXCLUDE operators), `None` when SQL NULL.
    pub conexclop: Option<OidArray>,
    /// `conbin` (`pg_node_tree` CHECK expression), `None` when SQL NULL.
    pub conbin: Option<alloc::string::String>,
    /// `indnatts` of the `conindid` index (PRIMARY/UNIQUE INCLUDE rendering),
    /// `None` when the constraint has no backing index.
    pub indnatts: Option<i16>,
    /// `indkey` of the `conindid` index (PRIMARY/UNIQUE INCLUDE column
    /// numbers), `None` when the constraint has no backing index.
    pub indkey: Option<alloc::vec::Vec<i16>>,
    /// `indnullsnotdistinct` of the `conindid` index (UNIQUE NULLS NOT
    /// DISTINCT rendering), `None` when there is no backing index.
    pub indnullsnotdistinct: Option<bool>,
}
