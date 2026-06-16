#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// heap.c phrases its column-count sanity check as `natts < 0 || natts > MAX`;
// keep that exact two-comparison form rather than a RangeInclusive.
#![allow(clippy::manual_range_contains)]
// The shared `PgError` variant is large; matching the rest of the workspace.
#![allow(clippy::result_large_err)]

//! Owned-tree port of `backend/catalog/heap.c` (PostgreSQL 18.3) — code to
//! create and destroy POSTGRES heap relations.
//!
//! ## Scope landed in this pass
//!
//! heap.c is a ~4 100-line file. This pass lands the *validation +
//! system-attribute* layer, which is fully buildable against the repo's
//! already-ported foundation (lsyscache type lookups, format_type, the
//! relcache `relation_open`/`relation_close`) and is a hard prerequisite for
//! both `index.c` (`ConstructTupleDescriptor` calls `CheckAttributeType`) and
//! `tablecmds`:
//!
//!   * the hard-coded system-attribute table (`SysAtt`) plus
//!     [`SystemAttributeDefinition`] / [`SystemAttributeByName`];
//!   * the `RELKIND_HAS_*` predicate macros + `RelFileNumberIsValid`;
//!   * [`CheckAttributeNamesTypes`] / [`CheckAttributeType`] — the recursive
//!     attribute-name and datatype validation;
//!   * the `CHKATYPE_*` flag bits (`catalog/heap.h`).
//!
//! ## STOP — the catalog-WRITE families are keystone-blocked
//!
//! `heap_create`, `heap_create_with_catalog`, `AddNewRelation{Tuple,Type}`,
//! `InsertPg{Class,Attribute}Tuple(s)`, `AddNewAttributeTuples`, the
//! `heap_drop_with_catalog` / `heap_truncate*` / `Delete*Tuples` /
//! `RemoveAttributeById` families, the `Store*`/`AddRelationNewConstraints`
//! constraint writers, and `RelationClearMissing`/`StoreAttrMissingVal`/
//! `SetAttrMissing` are NOT landed here. They cannot be ported faithfully
//! until the K1–K3 catalog-write carrier keystones land (see this crate's
//! `audits/` note): the repo's `FormData_pg_class` carrier is trimmed to ~21
//! of the catalog's ~33 columns and has no INSERT producer, `FormData_pg_attrdef`
//! is absent, `RelationBuildLocalRelation` is a trimmed stub (drops the tupdesc
//! and `relam`), and there are no `catalog_tuple_insert_pg_class` /
//! `catalog_tuple_insert_pg_attribute` / `catalog_tuple_insert_pg_attrdef`
//! producers in `catalog-indexing`. Widening `FormData_pg_class` to the full
//! row ripples across ~66 reader crates + the relcache local-rel build and
//! demands a constant-table audit on the widened Form; that is a coordinated
//! keystone campaign, not a single-unit port.

use backend_utils_cache_lsyscache::namespace_range_index_pubsub::{
    get_range_collation, get_range_subtype,
};
use backend_utils_cache_lsyscache::type_::{
    get_base_type, get_element_type, get_typ_typrelid, get_typtype, type_is_collatable,
};
use backend_utils_error::ereport;
use mcx::Mcx;
use types_core::primitive::{
    AttrNumber, InvalidRelFileNumber, Oid, OidIsValid, RelFileNumber, TransactionId,
};
use types_error::ERROR;
use types_core::xact::CommandId;
use types_core::{FirstUnpinnedObjectId, NAMEDATALEN};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_TOO_MANY_COLUMNS,
};
use types_storage::lock::AccessShareLock;
use types_tuple::access::{
    ATTRIBUTE_GENERATED_VIRTUAL, RELKIND_COMPOSITE_TYPE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE, RELKIND_VIEW,
};
use types_tuple::heaptuple::{
    FormData_pg_attribute, ItemPointerData, MaxHeapAttributeNumber, NameData, ANYARRAYOID, CIDOID,
    OIDOID, RECORDARRAYOID, RECORDOID, TIDOID, XIDOID,
};

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
        let att_typelem = get_element_type(atttypid)?.unwrap_or(types_core::primitive::InvalidOid);
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
