#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// heap.c phrases its column-count sanity check as `natts < 0 || natts > MAX`;
// keep that exact two-comparison form rather than a RangeInclusive.
#![allow(clippy::manual_range_contains)]
// The shared `PgError` variant is large; matching the rest of the workspace.
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! Owned-tree port of `backend/catalog/heap.c` (PostgreSQL 18.3) — code to
//! create and destroy POSTGRES heap relations.
//!
//! ## Scope landed in this pass
//!
//! The **relation-creation core** plus the validation/system-attribute spine:
//!
//!   * the hard-coded system-attribute table (`SysAtt`) plus
//!     [`SystemAttributeDefinition`] / [`SystemAttributeByName`];
//!   * the `RELKIND_HAS_*` predicate macros + `RelFileNumberIsValid`;
//!   * [`CheckAttributeNamesTypes`] / [`CheckAttributeType`];
//!   * [`heap_create`] — relcache local-relation build + physical-storage
//!     orchestration;
//!   * [`heap_create_with_catalog`] — the full cataloged-relation create:
//!     OID allocation, rowtype + array pg_type creation (`AddNewRelationType` →
//!     `TypeCreate`), the pg_class row (`AddNewRelationTuple` /
//!     `InsertPgClassTuple` → `catalog_tuple_insert_pg_class`), the
//!     pg_attribute rows (`AddNewAttributeTuples` / `InsertPgAttributeTuples` →
//!     `catalog_insert_pg_attribute_tuples`), dependency recording, the
//!     post-create hook, the (NIL) constraint store, and the ON COMMIT
//!     registration;
//!   * the catalog-row **delete** family `DeleteRelationTuple` /
//!     `DeleteAttributeTuples` / `DeleteSystemAttributeTuples`;
//!   * the **drop** family [`heap_drop_with_catalog`] (the full relation-drop
//!     orchestration — wired as the inward seam dependency.c reaches), plus its
//!     [`RemovePartitionKeyByRelId`] and [`RemoveStatistics`] collaborators.
//!
//! This builds on the K1/K2/K3 catalog-write carrier keystone: the full-row
//! INSERT carriers (`PgClassInsertRow`, `PgAttributeInsertRow`) and their
//! producers in `backend-catalog-indexing` form the heap tuples; the trimmed
//! relcache `rd_rel` carries the read-side fields.
//!
//! ## Constraint-cooker / attribute-mutate families (landed in `constraints.rs`)
//!
//! The constraint cooker is now ported faithfully (see `constraints.rs`):
//! `cookDefault` / `cookConstraint` (working in `Expr`, wrapped to `Node::Expr`
//! at the storage boundary), the generated-column walkers
//! `check_nested_generated` / `check_virtual_generated_security`, the writers
//! `StoreRelCheck` / `StoreRelNotNull` / `StoreConstraints` /
//! `AddRelationNewConstraints` / `AddRelationNotNullConstraints`, and
//! `SetRelationNumChecks`. The `add_relation_new_constraints` /
//! `add_relation_not_null_constraints` outward seams (the live tablecmds
//! CREATE-TABLE consumer) are INSTALLED here. `CopyStatistics` is ported in
//! `statistics.rs` (heap_modify_tuple-on-column-1 + `CatalogTupleInsertWithInfo`).
//!
//! Mirror-and-panic boundaries (genuinely-unported carriers, loud panic):
//!   * `MergeWithExistingConstraint`'s pg_constraint lookup + `conbin` reader +
//!     field-update (`merge_with_existing_constraint` seam);
//!   * `SetRelationNumChecks`'s disk-store branch (the trimmed `PgClassForm`
//!     carries no `relchecks`; `set_relation_num_checks` seam) — the
//!     `relchecks == numchecks` `CacheInvalidate` branch is real;
//!   * `RelationClearMissing` is REAL in-crate (systable scan on pg_attribute
//!     by `attrelid` + `heap_modify_tuple` clearing `atthasmissing` / nulling
//!     `attmissingval` + `CatalogTupleUpdate`); the inward
//!     `relation_clear_missing` entry seam is INSTALLED.
//!   * `RemoveAttributeById` / `StoreAttrMissingVal` need a writable full-row
//!     `ATTNUM` syscache copy + a `pg_attribute` `CatalogTupleUpdate` carrier
//!     (and `construct_array`-of-missingval) (`remove_attribute_by_id_update` /
//!     `store_attr_missing_val` seams). The inward `RemoveAttributeById` entry
//!     seam is INSTALLED and runs the real `RemoveStatistics` half in-crate.
//!     `SetAttrMissing` (binary-upgrade only, no in-tree caller) is deferred.
//!
//! ## partition-store (key) landed; bound / truncate families carrier-blocked
//!
//! `StorePartitionKey` IS ported (see `partition.rs`): it builds the
//! `int2vector`/`oidvector`/`pg_node_tree` images inline (the same byte layout
//! `backend-catalog-indexing` uses), forms + inserts the `pg_partitioned_table`
//! row, records the opclass/collation/column dependencies, and invalidates the
//! relcache.
//!
//! `StorePartitionBound` remains unported: it rewrites `pg_class.relpartbound`
//! from a transformed `PartitionBoundSpec`, depending on the partition-bound
//! transform/validation machinery (`transformPartitionBound` /
//! `check_new_partition_bound` in `partitioning/partbounds.c`) and the partition
//! descriptor (`partitioning/partdesc.c`), neither of which is ported yet.
//! `heap_truncate` / `heap_truncate_one_rel` / `RelationTruncateIndexes` need
//! `table_relation_nontransactional_truncate` (no tableam seam) +
//! `BuildDummyIndexInfo` (only `BuildIndexInfo` exists). These remain unported
//! (no stub).
//!
//! `heap_truncate_find_FKs` / `heap_truncate_check_FKs` (the TRUNCATE FK-check
//! tail) ARE ported in `truncate.rs` and their `backend-commands-tablecmds-seams`
//! seams installed: the `pg_constraint` full seqscan+deform is genam-owned
//! (`scan_pg_constraint_truncate_fks`), the relids-only seam reads
//! `relhastriggers`/`relkind` via lsyscache.

extern crate alloc;

use alloc::vec::Vec;

use backend_utils_cache_lsyscache::namespace_range_index_pubsub::{
    get_range_collation, get_range_subtype,
};
use backend_utils_cache_lsyscache::type_::{
    get_base_type, get_element_type, get_typ_typrelid, get_typtype, type_is_collatable,
};
use backend_utils_error::ereport;
use mcx::Mcx;
use types_core::primitive::{
    AttrNumber, InvalidOid, InvalidRelFileNumber, Oid, OidIsValid, RelFileNumber, TransactionId,
};
use types_core::xact::{CommandId, InvalidTransactionId};
use types_core::{FirstUnpinnedObjectId, NAMEDATALEN};
use types_error::ERROR;
use types_error::{
    PgResult, ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_TOO_MANY_COLUMNS,
};
use types_storage::lock::{AccessShareLock, NoLock};
use types_tuple::access::{
    ATTRIBUTE_GENERATED_VIRTUAL, RELKIND_COMPOSITE_TYPE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE, RELKIND_VIEW,
};
use types_tuple::heaptuple::{
    FormData_pg_attribute, ItemPointerData, MaxHeapAttributeNumber, NameData, TupleDescData,
    ANYARRAYOID, CIDOID, OIDOID, RECORDARRAYOID, RECORDOID, TIDOID, XIDOID,
};

pub mod inward;
pub use inward::init_seams;

/* ----------------------------------------------------------------
 * `catalog/heap.h` flag bits for CheckAttributeType / CheckAttributeNamesTypes.
 * ---------------------------------------------------------------- */

/// `CHKATYPE_ANYARRAY` — allow ANYARRAY.
pub const CHKATYPE_ANYARRAY: i32 = 1 << 0;
/// `CHKATYPE_ANYRECORD` — allow RECORD and RECORD[].
pub const CHKATYPE_ANYRECORD: i32 = 1 << 1;
/// `CHKATYPE_IS_PARTKEY` — attname is a partitioning column.
pub const CHKATYPE_IS_PARTKEY: i32 = 1 << 2;
/// `CHKATYPE_IS_VIRTUAL` — column is virtual generated.
pub const CHKATYPE_IS_VIRTUAL: i32 = 1 << 3;

/* pg_type typtype codes (catalog/pg_type.h). `get_typtype` yields a `u8`. */
const TYPTYPE_COMPOSITE: u8 = b'c';
const TYPTYPE_DOMAIN: u8 = b'd';
const TYPTYPE_PSEUDO: u8 = b'p';
const TYPTYPE_RANGE: u8 = b'r';

/* Storage / alignment codes (catalog/pg_attribute.h, c.h). */
const TYPSTORAGE_PLAIN: i8 = b'p' as i8;
const TYPALIGN_INT: i8 = b'i' as i8;
const TYPALIGN_SHORT: i8 = b's' as i8;
const TYPALIGN_DOUBLE: i8 = b'd' as i8;
const TYPSTORAGE_EXTENDED: i8 = b'x' as i8;

/* pg_type typtype / category codes used by AddNewRelationType / TypeCreate. */
const TYPTYPE_COMPOSITE_C: i8 = b'c' as i8;
const TYPTYPE_BASE_C: i8 = b'b' as i8;
const TYPCATEGORY_COMPOSITE: i8 = b'C' as i8;
const TYPCATEGORY_ARRAY: i8 = b'A' as i8;
const DEFAULT_TYPDELIM: i8 = b',' as i8;

/* Built-in fmgr OIDs for the record / array I/O procs (utils/fmgroids.h). */
const F_RECORD_IN: Oid = 2290;
const F_RECORD_OUT: Oid = 2291;
const F_RECORD_RECV: Oid = 2402;
const F_RECORD_SEND: Oid = 2403;
const F_ARRAY_IN: Oid = 750;
const F_ARRAY_OUT: Oid = 751;
const F_ARRAY_RECV: Oid = 2400;
const F_ARRAY_SEND: Oid = 2401;
const F_ARRAY_TYPANALYZE: Oid = 3816;
const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;

/* System-catalog OIDs (genbki). */
const RelationRelationId: Oid = 1259;
const AttributeRelationId: Oid = 1249;
const TypeRelationId: Oid = 1247;
const NamespaceRelationId: Oid = 2615;
const CollationRelationId: Oid = 3456;
const AccessMethodRelationId: Oid = 2601;
const InheritsRelationId: Oid = 2611;
const GLOBALTABLESPACE_OID: Oid = 1664;

/* System-attribute numbers (access/sysattr.h). */
const SelfItemPointerAttributeNumber: AttrNumber = -1;
const MinTransactionIdAttributeNumber: AttrNumber = -2;
const MinCommandIdAttributeNumber: AttrNumber = -3;
const MaxTransactionIdAttributeNumber: AttrNumber = -4;
const MaxCommandIdAttributeNumber: AttrNumber = -5;
const TableOidAttributeNumber: AttrNumber = -6;

/* ----------------------------------------------------------------
 *				XXX UGLY HARD CODED BADNESS FOLLOWS XXX
 *
 *		these should all be moved to someplace in the lib/catalog
 *		module, if not obliterated first.
 * ----------------------------------------------------------------
 */

/// Build a `NameData` from a fixed name (the `.attname = {"ctid"}` initializer
/// form in the C — a NUL-padded fixed-size C string).
fn name_data(s: &[u8]) -> NameData {
    let mut data = [0u8; NAMEDATALEN as usize];
    debug_assert!(s.len() <= NAMEDATALEN as usize);
    data[..s.len()].copy_from_slice(s);
    NameData { data }
}

/// `namestrcpy(&name, s)` — copy `s` (truncated to `NAMEDATALEN - 1` bytes on a
/// UTF-8 boundary) into a NUL-padded 64-byte `NameData` image.
fn namestrcpy(s: &str) -> [u8; 64] {
    let mut data = [0u8; NAMEDATALEN as usize];
    let limit = (NAMEDATALEN - 1) as usize;
    let mut end = limit.min(s.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    data[..end].copy_from_slice(&s.as_bytes()[..end]);
    data
}

/// `SysAtt[]` — the system-attribute prototype `Form_pg_attribute`s, in the C's
/// `{&a1, &a2, &a3, &a4, &a5, &a6}` order (`ctid`, `xmin`, `cmin`, `xmax`,
/// `cmax`, `tableoid`). The initializers omit trailing variable-length fields
/// and any zero-defaulted fields, exactly as the C comment notes.
fn sys_att() -> [FormData_pg_attribute; 6] {
    let mk = |name: &[u8],
              atttypid: Oid,
              attlen: i16,
              attnum: AttrNumber,
              attbyval: bool,
              attalign: i8| FormData_pg_attribute {
        attname: name_data(name),
        atttypid,
        attlen,
        attnum,
        atttypmod: -1,
        attbyval,
        attalign,
        attstorage: TYPSTORAGE_PLAIN,
        attnotnull: true,
        attislocal: true,
        ..FormData_pg_attribute::default()
    };

    // sizeof(ItemPointerData) == 6, sizeof(TransactionId)/CommandId/Oid == 4.
    let a1 = mk(
        b"ctid",
        TIDOID,
        core::mem::size_of::<ItemPointerData>() as i16,
        SelfItemPointerAttributeNumber,
        false,
        TYPALIGN_SHORT,
    );
    let a2 = mk(
        b"xmin",
        XIDOID,
        core::mem::size_of::<TransactionId>() as i16,
        MinTransactionIdAttributeNumber,
        true,
        TYPALIGN_INT,
    );
    let a3 = mk(
        b"cmin",
        CIDOID,
        core::mem::size_of::<CommandId>() as i16,
        MinCommandIdAttributeNumber,
        true,
        TYPALIGN_INT,
    );
    let a4 = mk(
        b"xmax",
        XIDOID,
        core::mem::size_of::<TransactionId>() as i16,
        MaxTransactionIdAttributeNumber,
        true,
        TYPALIGN_INT,
    );
    let a5 = mk(
        b"cmax",
        CIDOID,
        core::mem::size_of::<CommandId>() as i16,
        MaxCommandIdAttributeNumber,
        true,
        TYPALIGN_INT,
    );
    // We decided to call this attribute "tableoid" rather than say "classoid".
    let a6 = mk(
        b"tableoid",
        OIDOID,
        core::mem::size_of::<Oid>() as i16,
        TableOidAttributeNumber,
        true,
        TYPALIGN_INT,
    );

    [a1, a2, a3, a4, a5, a6]
}

/// This function returns a `Form_pg_attribute` for a system attribute. Note
/// that we elog if the presented attno is invalid, which would only happen if
/// there's a problem upstream.
pub fn SystemAttributeDefinition(attno: AttrNumber) -> PgResult<FormData_pg_attribute> {
    let sys_att = sys_att();
    if attno >= 0 || (attno as i32) < -(sys_att.len() as i32) {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("invalid system attribute number {attno}"))
            .into_error());
    }
    Ok(sys_att[(-attno - 1) as usize])
}

/// If the given name is a system attribute name, return a `Form_pg_attribute`
/// for a prototype definition. If not, return `None`.
pub fn SystemAttributeByName(attname: &[u8]) -> Option<FormData_pg_attribute> {
    let sys_att = sys_att();
    for att in sys_att.iter() {
        if att.attname.name_str() == attname {
            return Some(*att);
        }
    }
    None
}

/* ----------------------------------------------------------------
 *				XXX END OF UGLY HARD CODED BADNESS XXX
 * ---------------------------------------------------------------- */

/* ===========================================================================
 * RELKIND_HAS_* predicate macros (catalog/pg_class.h), inlined 1:1.
 * ========================================================================= */

/// `RELKIND_HAS_STORAGE(relkind)` (`catalog/pg_class.h`).
pub fn RELKIND_HAS_STORAGE(relkind: u8) -> bool {
    relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
}

/// `RELKIND_HAS_PARTITIONS(relkind)` (`catalog/pg_class.h`).
pub fn RELKIND_HAS_PARTITIONS(relkind: u8) -> bool {
    relkind == RELKIND_PARTITIONED_TABLE || relkind == RELKIND_PARTITIONED_INDEX
}

/// `RELKIND_HAS_TABLESPACE(relkind)` (`catalog/pg_class.h`).
pub fn RELKIND_HAS_TABLESPACE(relkind: u8) -> bool {
    (RELKIND_HAS_STORAGE(relkind) || RELKIND_HAS_PARTITIONS(relkind)) && relkind != RELKIND_SEQUENCE
}

/// `RELKIND_HAS_TABLE_AM(relkind)` (`catalog/pg_class.h`).
pub fn RELKIND_HAS_TABLE_AM(relkind: u8) -> bool {
    relkind == RELKIND_RELATION || relkind == RELKIND_TOASTVALUE || relkind == RELKIND_MATVIEW
}

/// `RelFileNumberIsValid(rnum)` (`storage/relfilelocator.h`).
pub fn RelFileNumberIsValid(rnum: RelFileNumber) -> bool {
    rnum != InvalidRelFileNumber
}

/// `format_type_be(type_oid)` — the type's printable name for an error message.
fn format_type_be(type_oid: Oid) -> PgResult<String> {
    backend_utils_adt_format_type_seams::format_type_be_owned::call(type_oid)
}

/// `format_type_be(type_oid)` — crate-internal re-export for sibling modules
/// (`constraints.rs`'s `cookDefault` error messages).
pub(crate) fn format_type_be_pub(type_oid: Oid) -> PgResult<String> {
    format_type_be(type_oid)
}

/* --------------------------------
 *		CheckAttributeNamesTypes
 *
 *		this is used to make certain the tuple descriptor contains a valid set
 *		of attribute names and datatypes.  a problem simply generates
 *		ereport(ERROR) which aborts the current transaction.
 *
 *		relkind is the relkind of the relation to be created.
 *		flags controls which datatypes are allowed, cf CheckAttributeType.
 * --------------------------------
 */
pub fn CheckAttributeNamesTypes<'mcx>(
    mcx: Mcx<'mcx>,
    attrs: &[FormData_pg_attribute],
    relkind: u8,
    flags: i32,
) -> PgResult<()> {
    let natts: i32 = attrs.len() as i32;

    /* Sanity check on column count */
    if natts < 0 || natts > MaxHeapAttributeNumber {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(format!(
                "tables can have at most {MaxHeapAttributeNumber} columns"
            ))
            .into_error());
    }

    /*
     * first check for collision with system attribute names
     *
     * Skip this for a view or type relation, since those don't have system
     * attributes.
     */
    if relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE {
        for attr in attrs.iter() {
            if SystemAttributeByName(attr.attname.name_str()).is_some() {
                let attname = String::from_utf8_lossy(attr.attname.name_str()).into_owned();
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_COLUMN)
                    .errmsg(format!(
                        "column name \"{attname}\" conflicts with a system column name"
                    ))
                    .into_error());
            }
        }
    }

    /*
     * next check for repeated attribute names
     */
    for i in 1..natts as usize {
        for j in 0..i {
            if attrs[j].attname.name_str() == attrs[i].attname.name_str() {
                let dup = String::from_utf8_lossy(attrs[j].attname.name_str()).into_owned();
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_COLUMN)
                    .errmsg(format!("column name \"{dup}\" specified more than once"))
                    .into_error());
            }
        }
    }

    /*
     * next check the attribute types
     */
    for attr in attrs.iter() {
        let attname = String::from_utf8_lossy(attr.attname.name_str()).into_owned();
        let mut containing_rowtypes: Vec<Oid> = Vec::new();
        CheckAttributeType(
            mcx,
            &attname,
            attr.atttypid,
            attr.attcollation,
            &mut containing_rowtypes, // assume we're creating a new rowtype
            flags
                | if attr.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
                    CHKATYPE_IS_VIRTUAL
                } else {
                    0
                },
        )?;
    }

    Ok(())
}

/* --------------------------------
 *		CheckAttributeType
 *
 *		Verify that the proposed datatype of an attribute is legal.
 *		This is needed mainly because there are types (and pseudo-types)
 *		in the catalogs that we do not support as elements of real tuples.
 *		We also check some other properties required of a table column.
 * --------------------------------
 */
pub fn CheckAttributeType<'mcx>(
    mcx: Mcx<'mcx>,
    attname: &str,
    atttypid: Oid,
    attcollation: Oid,
    containing_rowtypes: &mut Vec<Oid>,
    flags: i32,
) -> PgResult<()> {
    let att_typtype = get_typtype(atttypid)?;

    /* since this function recurses, it could be driven to stack overflow */
    backend_utils_misc_stack_depth::check_stack_depth()?;

    if att_typtype == TYPTYPE_PSEUDO {
        /*
         * We disallow pseudo-type columns, with the exception of ANYARRAY,
         * RECORD, and RECORD[] when the caller says that those are OK.
         *
         * We don't need to worry about recursive containment for RECORD and
         * RECORD[] because (a) no named composite type should be allowed to
         * contain those, and (b) two "anonymous" record types couldn't be
         * considered to be the same type, so infinite recursion isn't possible.
         */
        if !((atttypid == ANYARRAYOID && (flags & CHKATYPE_ANYARRAY) != 0)
            || (atttypid == RECORDOID && (flags & CHKATYPE_ANYRECORD) != 0)
            || (atttypid == RECORDARRAYOID && (flags & CHKATYPE_ANYRECORD) != 0))
        {
            if (flags & CHKATYPE_IS_PARTKEY) != 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                    // translator: first %s is an integer not a name
                    .errmsg(format!(
                        "partition key column {attname} has pseudo-type {}",
                        format_type_be(atttypid)?
                    ))
                    .into_error());
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                    .errmsg(format!(
                        "column \"{attname}\" has pseudo-type {}",
                        format_type_be(atttypid)?
                    ))
                    .into_error());
            }
        }
    } else if att_typtype == TYPTYPE_DOMAIN {
        /*
         * Prevent virtual generated columns from having a domain type. We would
         * have to enforce domain constraints when columns underlying the
         * generated column change. This could possibly be implemented, but it's
         * not.
         */
        if (flags & CHKATYPE_IS_VIRTUAL) != 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "virtual generated column \"{attname}\" cannot have a domain type"
                ))
                .into_error());
        }

        /*
         * If it's a domain, recurse to check its base type.
         */
        CheckAttributeType(
            mcx,
            attname,
            get_base_type(atttypid)?,
            attcollation,
            containing_rowtypes,
            flags,
        )?;
    } else if att_typtype == TYPTYPE_COMPOSITE {
        /*
         * For a composite type, recurse into its attributes.
         */

        /*
         * Check for self-containment. Eventually we might be able to allow this
         * (just return without complaint, if so) but it's not clear how many
         * other places would require anti-recursion defenses before it would be
         * safe to allow tables to contain their own rowtype.
         */
        if containing_rowtypes.contains(&atttypid) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "composite type {} cannot be made a member of itself",
                    format_type_be(atttypid)?
                ))
                .into_error());
        }

        containing_rowtypes.push(atttypid);

        let relation = backend_access_common_relation::relation_open(
            mcx,
            get_typ_typrelid(atttypid)?,
            AccessShareLock,
        )?;

        // RelationGetDescr(relation) — rel.h `rd_att` direct read.
        let natts = relation.rd_att.natts;
        for i in 0..natts as usize {
            let attr = relation.rd_att.attr(i);

            if attr.attisdropped {
                continue;
            }
            let inner_name = String::from_utf8_lossy(attr.attname.name_str()).into_owned();
            let inner_typid = attr.atttypid;
            let inner_collation = attr.attcollation;
            CheckAttributeType(
                mcx,
                &inner_name,
                inner_typid,
                inner_collation,
                containing_rowtypes,
                flags & !CHKATYPE_IS_PARTKEY,
            )?;
        }

        relation.close(AccessShareLock)?;

        /* list_delete_last(containing_rowtypes) */
        containing_rowtypes.pop();
    } else if att_typtype == TYPTYPE_RANGE {
        /*
         * If it's a range, recurse to check its subtype.
         */
        CheckAttributeType(
            mcx,
            attname,
            get_range_subtype(atttypid)?,
            get_range_collation(atttypid)?,
            containing_rowtypes,
            flags,
        )?;
    } else {
        let att_typelem = get_element_type(atttypid)?.unwrap_or(InvalidOid);
        if OidIsValid(att_typelem) {
            /*
             * Must recurse into array types, too, in case they are composite.
             */
            CheckAttributeType(
                mcx,
                attname,
                att_typelem,
                attcollation,
                containing_rowtypes,
                flags,
            )?;
        }
    }

    /*
     * For consistency with check_virtual_generated_security().
     */
    if (flags & CHKATYPE_IS_VIRTUAL) != 0 && atttypid >= FirstUnpinnedObjectId {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "virtual generated column \"{attname}\" cannot have a user-defined type"
            ))
            .errdetail(
                "Virtual generated columns that make use of user-defined types are not yet supported.",
            )
            .into_error());
    }

    /*
     * This might not be strictly invalid per SQL standard, but it is pretty
     * useless, and it cannot be dumped, so we must disallow it.
     */
    if !OidIsValid(attcollation) && type_is_collatable(atttypid)? {
        if (flags & CHKATYPE_IS_PARTKEY) != 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                // translator: first %s is an integer not a name
                .errmsg(format!(
                    "no collation was derived for partition key column {attname} with collatable type {}",
                    format_type_be(atttypid)?
                ))
                .errhint("Use the COLLATE clause to set the collation explicitly.")
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "no collation was derived for column \"{attname}\" with collatable type {}",
                    format_type_be(atttypid)?
                ))
                .errhint("Use the COLLATE clause to set the collation explicitly.")
                .into_error());
        }
    }

    Ok(())
}

/* ----------------------------------------------------------------
 *		heap_create		- Create an uncataloged heap relation
 * ---------------------------------------------------------------- */

/// `heap_create`'s frozen-xid / created-relation carriers. Defined in the
/// seams crate (so the cross-unit `heap_create` seam can name them without an
/// owner dependency); re-exported here for the owner-internal callers.
pub use backend_catalog_heap_seams::{HeapCreateResult, HeapCreateXids};

/// `heap_create` — create an uncataloged heap relation.
///
/// Note API change: the caller must now always provide the OID to use for the
/// relation. The relfilenumber may be (and in the simplest cases is) left
/// unspecified. `create_storage` indicates whether or not to create the
/// storage; however, even if `create_storage` is true, no storage will be
/// created if the relkind is one that doesn't have storage. `rel->rd_rel` is
/// initialized by `RelationBuildLocalRelation`, and is mostly zeroes at return.
pub fn heap_create<'mcx>(
    mcx: Mcx<'mcx>,
    relname: &str,
    relnamespace: Oid,
    mut reltablespace: Oid,
    relid: Oid,
    mut relfilenumber: RelFileNumber,
    accessmtd: Oid,
    tup_desc: &TupleDescData<'_>,
    relkind: u8,
    relpersistence: u8,
    shared_relation: bool,
    mapped_relation: bool,
    allow_system_table_mods: bool,
    mut create_storage: bool,
) -> PgResult<HeapCreateResult> {
    // The caller must have provided an OID for the relation.
    debug_assert!(OidIsValid(relid));

    /*
     * Don't allow creating relations in pg_catalog directly, even though it is
     * allowed to move user defined relations there. Semantics with search
     * paths including pg_catalog are too confusing for now.
     *
     * But allow creating indexes on relations in pg_catalog even if
     * allow_system_table_mods = off, upper layers already guarantee it's on a
     * user defined relation, not a system one.
     */
    if !allow_system_table_mods
        && ((backend_catalog_catalog::IsCatalogNamespace(relnamespace) && relkind != RELKIND_INDEX)
            || backend_catalog_catalog::IsToastNamespace(relnamespace))
        && backend_utils_init_miscinit::IsNormalProcessingMode()
    {
        let nsp =
            backend_utils_cache_lsyscache::namespace_range_index_pubsub::get_namespace_name(
                mcx,
                relnamespace,
            )?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied to create \"{nsp}.{relname}\""))
            .errdetail("System catalog modifications are currently disallowed.")
            .into_error());
    }

    let mut xids = HeapCreateXids {
        relfrozenxid: InvalidTransactionId,
        relminmxid: 0, // InvalidMultiXactId
    };

    /*
     * Force reltablespace to zero if the relation kind does not support
     * tablespaces. This is mainly just for cleanliness' sake.
     */
    if !RELKIND_HAS_TABLESPACE(relkind) {
        reltablespace = InvalidOid;
    }

    /* Don't create storage for relkinds without physical storage. */
    if !RELKIND_HAS_STORAGE(relkind) {
        create_storage = false;
    } else {
        /*
         * If relfilenumber is unspecified by the caller then create storage
         * with oid same as relid.
         */
        if !RelFileNumberIsValid(relfilenumber) {
            relfilenumber = relid;
        }
    }

    /*
     * Never allow a pg_class entry to explicitly specify the database's default
     * tablespace in reltablespace; force it to zero instead. This ensures that
     * if the database is cloned with a different default tablespace, the
     * pg_class entry will still match where CREATE DATABASE will put the
     * physically copied relation.
     *
     * Yes, this is a bit of a hack.
     */
    if reltablespace == backend_commands_tablespace_globals_seams::MyDatabaseTableSpace::call()? {
        reltablespace = InvalidOid;
    }

    /*
     * build the relcache entry.
     */
    let rel = backend_utils_cache_relcache::initfile::RelationBuildLocalRelation(
        relname,
        relnamespace,
        tup_desc,
        relid,
        accessmtd,
        reltablespace,
        shared_relation,
        mapped_relation,
        relpersistence as i8,
        relkind as i8,
        relfilenumber,
    )?;

    // Open the just-built relcache entry to read its `rd_rel`/`rd_locator` for
    // the storage providers below (C: `new_rel_desc->rd_rel->relkind`, the
    // `rd_locator` smgr uses). The entry was already pinned once by
    // `RelationBuildLocalRelation`'s `RelationIncrementReferenceCount`; open it
    // PIN-FREE so closing this handle (`rel_data.close(NoLock)` below) releases
    // exactly that single build pin — mirroring C's `heap_create` returning the
    // pinned `new_rel_desc` and `heap_create_with_catalog` closing it with
    // `table_close(new_rel_desc, NoLock)`. A normal `relation_open` here would
    // take a second pin, leaving the build pin stuck (a later DROP/TRUNCATE
    // `CheckTableNotInUse` would see refcnt > expected and fail).
    let rel_data = backend_access_common_relation::relation_open_prebuilt(mcx, rel)?;

    /*
     * Have the storage manager create the relation's disk file, if needed.
     *
     * For tables, the AM callback creates both the main and the init fork. For
     * others, only the main fork is created; the other forks will be created on
     * demand.
     */
    if create_storage {
        let rd_relkind = rel_data.rd_rel.relkind;
        if RELKIND_HAS_TABLE_AM(rd_relkind) {
            // table_relation_set_new_filelocator(rel, &rel->rd_locator,
            //     relpersistence, &relfrozenxid, &relminmxid);
            let (fx, mm) =
                backend_access_table_tableam_seams::table_relation_set_new_filelocator::call(
                    &rel_data,
                    rel_data.rd_locator,
                    relpersistence as i8,
                )?;
            xids.relfrozenxid = fx;
            xids.relminmxid = mm;
        } else if RELKIND_HAS_STORAGE(rd_relkind) {
            // RelationCreateStorage(rel->rd_locator, relpersistence, true);
            backend_catalog_storage_seams::relation_create_storage_main_fork::call(
                rel_data.rd_locator,
                relpersistence as i8,
            )?;
        } else {
            debug_assert!(false);
        }
    }

    /*
     * If a tablespace is specified, removal of that tablespace is normally
     * protected by the existence of a physical file; but for relations with no
     * files, add a pg_shdepend entry to account for that.
     */
    if !create_storage && reltablespace != InvalidOid {
        backend_catalog_pg_shdepend_seams::recordDependencyOnTablespace::call(
            RelationRelationId,
            relid,
            reltablespace,
        )?;
    }

    /* ensure that stats are dropped if transaction aborts */
    backend_utils_activity_pgstat_seams::pgstat_create_relation::call(
        rel_data.rd_id,
        rel_data.rd_rel.relisshared,
    )?;

    rel_data.close(NoLock)?;

    Ok(HeapCreateResult { rel, xids })
}

mod constraints;
mod create;
mod delete;
mod drop;
mod partition;
mod statistics;
mod truncate;

pub use truncate::{heap_truncate_check_FKs, heap_truncate_find_FKs};

pub use create::{
    heap_create_with_catalog, AddNewAttributeTuples, AddNewRelationTuple, AddNewRelationType,
    InsertPgAttributeTuples, InsertPgClassTuple,
};
pub use delete::{
    DeleteAttributeTuples, DeleteRelationTuple, DeleteSystemAttributeTuples,
    RelationRemoveInheritance,
};
pub use drop::heap_drop_with_catalog;
pub use partition::{RemovePartitionKeyByRelId, StorePartitionKey};
pub use statistics::{CopyStatistics, RemoveStatistics};

pub use constraints::{
    AddRelationNewConstraints, AddRelationNotNullConstraints, ClearAttributeHasDefault,
    RelationClearMissing, RemoveAttributeById, SetAttributeHasDefault, StoreAttrMissingVal,
    StoreConstraints,
};
