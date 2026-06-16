//! `src/backend/catalog/pg_enum.c` (PostgreSQL 18.3) — routines to support
//! manipulation of the `pg_enum` relation.
//!
//! Ported 1:1 against the C, name-for-name. Catalog access mirrors the
//! `backend-catalog-pg-constraint` precedent: `table_open`/`close` guard the
//! relation; the member list the C reads via `SearchSysCacheList1(
//! ENUMTYPOIDNAME, …)` / the dup check via `SearchSysCache2(ENUMTYPOIDNAME,
//! …)` are `systable` scans on `EnumTypIdLabelIndexId`; tuple build/insert/
//! update/delete cross the per-`pg_enum` heapam seams. The two
//! transaction-lifespan hash tables (`uncommitted_enum_types`/`_values`,
//! cleared by `AtEOXact_Enum`) are modeled as backend-local `thread_local`
//! `Oid` sets (the C `HTAB` in `TopTransactionContext`).
//!
//! This crate OWNS the inward seams `at_eoxact_enum` (consumed by xact.c) and
//! `scan_enum_members` (consumed by typcache.c), installed in `init_seams()`.
//! It also installs the parallel-DSM `estimate/serialize/restore_uncommitted_
//! enums` seams (declared on `backend-access-transam-parallel-rt-seams`) that
//! parallel.c consumes — these are pg_enum.c functions.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use core::cell::RefCell;
use std::collections::HashSet;

use mcx::{Mcx, MemoryContext};

use types_catalog::catalog::TYPE_RELATION_ID;
use types_catalog::pg_enum::{
    Anum_pg_enum_enumlabel, Anum_pg_enum_enumsortorder, Anum_pg_enum_enumtypid, Anum_pg_enum_oid,
    EnumOidIndexId, EnumRelationId, EnumTypIdLabelIndexId, PgEnumInsertRow,
};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_core::Size;
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INVALID_NAME,
    ERRCODE_INVALID_PARAMETER_VALUE, ERROR, NOTICE};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{ExclusiveLock, RowExclusiveLock};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table as table;
use backend_access_transam_xact_seams as xact_seams;
use backend_catalog_binary_upgrade_seams as binary_upgrade_seams;
use backend_catalog_indexing_seams as indexing_seams;
use backend_storage_lmgr_lmgr_seams as lmgr_seams;

/// `NAMEDATALEN`.
const NAMEDATALEN: usize = 64;

/// `F_OIDEQ` (`catalog/fmgroids.h`).
use types_core::fmgr::F_OIDEQ;

/* ===========================================================================
 * Transaction-lifespan uncommitted-enum bookkeeping (the C HTABs).
 * ========================================================================= */

thread_local! {
    /// `uncommitted_enum_types` — OIDs of enum *types* made in the current
    /// transaction (`None` == the C NULL pointer, no table created yet).
    static UNCOMMITTED_ENUM_TYPES: RefCell<Option<HashSet<Oid>>> = const { RefCell::new(None) };
    /// `uncommitted_enum_values` — OIDs of enum *values* created in the current
    /// transaction by `AddEnumLabel` (`None` == the C NULL pointer).
    static UNCOMMITTED_ENUM_VALUES: RefCell<Option<HashSet<Oid>>> = const { RefCell::new(None) };
}

/// `init_uncommitted_enum_types()` — create the types table for this tx.
fn init_uncommitted_enum_types() {
    UNCOMMITTED_ENUM_TYPES.with(|t| {
        if t.borrow().is_none() {
            *t.borrow_mut() = Some(HashSet::new());
        }
    });
}

/// `init_uncommitted_enum_values()` — create the values table for this tx.
fn init_uncommitted_enum_values() {
    UNCOMMITTED_ENUM_VALUES.with(|t| {
        if t.borrow().is_none() {
            *t.borrow_mut() = Some(HashSet::new());
        }
    });
}

/// `EnumTypeUncommitted(typ_id)` — is the given type OID in the uncommitted
/// types table?
fn EnumTypeUncommitted(typ_id: Oid) -> bool {
    UNCOMMITTED_ENUM_TYPES.with(|t| match &*t.borrow() {
        Some(set) => set.contains(&typ_id),
        None => false,
    })
}

/// `EnumUncommitted(enum_id)` — is the given enum value OID in the uncommitted
/// values table?
pub fn EnumUncommitted(enum_id: Oid) -> bool {
    UNCOMMITTED_ENUM_VALUES.with(|t| match &*t.borrow() {
        Some(set) => set.contains(&enum_id),
        None => false,
    })
}

/// `AtEOXact_Enum()` — clean up enum stuff after end of top-level transaction.
/// The memory goes away with `TopTransactionContext`; we just clear our
/// pointers.
pub fn AtEOXact_Enum() {
    UNCOMMITTED_ENUM_TYPES.with(|t| *t.borrow_mut() = None);
    UNCOMMITTED_ENUM_VALUES.with(|t| *t.borrow_mut() = None);
}

/* ===========================================================================
 * scan-key + member-list helpers
 * ========================================================================= */

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: i16, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(value),
    )?;
    Ok(key)
}

/// One existing enum member (the fields `(Form_pg_enum) GETSTRUCT(tup)` reads,
/// plus the heap TID for `CatalogTupleUpdate`).
#[derive(Clone)]
struct EnumMember {
    oid: Oid,
    enumtypid: Oid,
    enumsortorder: f32,
    enumlabel: [u8; NAMEDATALEN],
    tid: ItemPointerData,
}

/// `NameStr(en->enumlabel)` — read the label up to its NUL.
fn name_str(name: &[u8; NAMEDATALEN]) -> &str {
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    core::str::from_utf8(&name[..end]).unwrap_or("")
}

/// `namestrcpy(&name, src)` — copy `src` into a zero-filled `NameData`.
fn namestrcpy(src: &str) -> [u8; NAMEDATALEN] {
    let mut name = [0u8; NAMEDATALEN];
    for (i, &byte) in src.as_bytes().iter().take(NAMEDATALEN).enumerate() {
        name[i] = byte;
    }
    name[NAMEDATALEN - 1] = 0;
    name
}

/// Read the `pg_enum` members of `enumTypeOid` (the C `SearchSysCacheList1(
/// ENUMTYPOIDNAME, enumTypeOid)` member list), via a `systable` scan on
/// `EnumTypIdLabelIndexId`.
fn list_enum_members<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::RelationData<'mcx>,
    enumTypeOid: Oid,
) -> PgResult<Vec<EnumMember>> {
    let key = [oid_key(Anum_pg_enum_enumtypid, enumTypeOid)?];
    let mut out: Vec<EnumMember> = Vec::new();
    let mut scan =
        genam_seams::systable_beginscan::call(rel, EnumTypIdLabelIndexId, true, None, &key)?;
    loop {
        let scratch = MemoryContext::new("pg_enum member scan row");
        let smcx = scratch.mcx();
        let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
            break;
        };
        let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let oid = cols[Anum_pg_enum_oid as usize - 1].0.as_oid();
        let enumtypid = cols[Anum_pg_enum_enumtypid as usize - 1].0.as_oid();
        let enumsortorder = cols[Anum_pg_enum_enumsortorder as usize - 1].0.as_f32();
        let enumlabel = {
            let mut n = [0u8; NAMEDATALEN];
            if let Datum::ByRef(b) = &cols[Anum_pg_enum_enumlabel as usize - 1].0 {
                let take = core::cmp::min(NAMEDATALEN, b.len());
                n[..take].copy_from_slice(&b[..take]);
            }
            n
        };
        let _ = mcx;
        out.push(EnumMember {
            oid,
            enumtypid,
            enumsortorder,
            enumlabel,
            tid: tup.tuple.t_self,
        });
    }
    scan.end()?;
    Ok(out)
}

/* ===========================================================================
 * EnumValuesCreate (pg_enum.c:83-216)
 * ========================================================================= */

/// EnumValuesCreate — create an entry in pg_enum for each of the supplied enum
/// values. `vals` is the list of label strings (a C `List *` of `String`s).
///
/// Assumed to be called only by CREATE TYPE AS ENUM, even when `vals` is empty,
/// so we enter the enum type's OID into `uncommitted_enum_types` here.
pub fn EnumValuesCreate(enumTypeOid: Oid, vals: &[&str]) -> PgResult<()> {
    /*
     * Remember the type OID as being made in the current transaction, but not
     * if we're in a subtransaction.
     */
    if xact_seams::get_current_transaction_nest_level::call() == 1 {
        init_uncommitted_enum_types();
        UNCOMMITTED_ENUM_TYPES.with(|t| {
            if let Some(set) = t.borrow_mut().as_mut() {
                set.insert(enumTypeOid);
            }
        });
    }

    let num_elems = vals.len();

    /*
     * We do not bother to check the list of values for duplicates --- the
     * unique index catches them.
     */
    let enum_ctx = MemoryContext::new("pg_enum");
    let pg_enum = table::table_open(enum_ctx.mcx(), EnumRelationId, RowExclusiveLock)?;

    /*
     * Allocate OIDs for the enum's members. We assign even-numbered OIDs to all
     * new enum labels so the comparison functions can compare directly.
     */
    let mut oids: Vec<Oid> = Vec::with_capacity(num_elems);
    for _ in 0..num_elems {
        let mut new_oid;
        loop {
            new_oid = indexing_seams::get_new_oid_with_index_pg_enum::call(&pg_enum)?;
            if new_oid & 1 == 0 {
                break;
            }
        }
        oids.push(new_oid);
    }

    /* sort them, just in case OID counter wrapped from high to low */
    oids.sort_unstable();

    /* and make the entries (build the rows, then batch-insert) */
    let mut rows: Vec<PgEnumInsertRow> = Vec::with_capacity(num_elems);
    for (elemno, lab) in vals.iter().enumerate() {
        /*
         * labels are stored in a name field, so check the length is within
         * range.
         */
        if lab.len() > NAMEDATALEN - 1 {
            return Err(PgError::new(ERROR, format!("invalid enum label \"{lab}\""))
                .with_sqlstate(ERRCODE_INVALID_NAME)
                .with_detail(format!("Labels must be {} bytes or less.", NAMEDATALEN - 1)));
        }

        rows.push(PgEnumInsertRow {
            oid: oids[elemno],
            enumtypid: enumTypeOid,
            enumsortorder: (elemno + 1) as f32,
            enumlabel: namestrcpy(lab),
        });
    }

    indexing_seams::catalog_tuples_multi_insert_pg_enum::call(&pg_enum, &rows)?;

    pg_enum.close(RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * EnumValuesDelete (pg_enum.c:223-249)
 * ========================================================================= */

/// EnumValuesDelete — remove all the pg_enum entries for the specified enum
/// type.
pub fn EnumValuesDelete(enumTypeOid: Oid) -> PgResult<()> {
    let enum_ctx = MemoryContext::new("pg_enum");
    let pg_enum = table::table_open(enum_ctx.mcx(), EnumRelationId, RowExclusiveLock)?;

    let key = [oid_key(Anum_pg_enum_enumtypid, enumTypeOid)?];
    let mut scan =
        genam_seams::systable_beginscan::call(&pg_enum, EnumTypIdLabelIndexId, true, None, &key)?;
    loop {
        let scratch = MemoryContext::new("pg_enum delete scan row");
        let Some(tup) = genam_seams::systable_getnext::call(scratch.mcx(), scan.desc_mut())? else {
            break;
        };
        indexing_seams::catalog_tuple_delete::call(&pg_enum, tup.tuple.t_self)?;
    }
    scan.end()?;

    pg_enum.close(RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * AddEnumLabel (pg_enum.c:291-599)
 * ========================================================================= */

/// AddEnumLabel — add a new label to the enum set. By default it goes at the
/// end, but the caller can place it before/after an existing member.
/// `neighbor` is `None` for "at the end".
pub fn AddEnumLabel(
    enumTypeOid: Oid,
    newVal: &str,
    neighbor: Option<&str>,
    newValIsAfter: bool,
    skipIfExists: bool,
) -> PgResult<()> {
    /* check length of new label is ok */
    if newVal.len() > NAMEDATALEN - 1 {
        return Err(PgError::new(ERROR, format!("invalid enum label \"{newVal}\""))
            .with_sqlstate(ERRCODE_INVALID_NAME)
            .with_detail(format!("Labels must be {} bytes or less.", NAMEDATALEN - 1)));
    }

    /*
     * Acquire a lock on the enum type, held until commit, so two backends
     * aren't concurrently modifying it.
     */
    lmgr_seams::lock_database_object::call(TYPE_RELATION_ID, enumTypeOid, 0, ExclusiveLock)?.keep();

    let enum_ctx = MemoryContext::new("pg_enum");
    let mcx = enum_ctx.mcx();

    /*
     * Check if label is already in use. The unique index would catch this, but
     * we prefer a friendlier message and need it for IF NOT EXISTS.
     */
    {
        let pg_enum_chk = table::table_open(mcx, EnumRelationId, RowExclusiveLock)?;
        let members = list_enum_members(mcx, &pg_enum_chk, enumTypeOid)?;
        pg_enum_chk.close(RowExclusiveLock)?;
        if members.iter().any(|m| name_str(&m.enumlabel) == newVal) {
            if skipIfExists {
                return Err(PgError::new(
                    NOTICE,
                    format!("enum label \"{newVal}\" already exists, skipping"),
                )
                .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
            } else {
                return Err(PgError::new(
                    ERROR,
                    format!("enum label \"{newVal}\" already exists"),
                )
                .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
            }
        }
    }

    let pg_enum = table::table_open(mcx, EnumRelationId, RowExclusiveLock)?;

    /* If we have to renumber, we restart from here. */
    let newOid;
    let newelemorder;
    'restart: loop {
        /* Get the existing members, sorted by enumsortorder. */
        let mut existing = list_enum_members(mcx, &pg_enum, enumTypeOid)?;
        existing.sort_by(|a, b| {
            a.enumsortorder
                .partial_cmp(&b.enumsortorder)
                .unwrap_or(core::cmp::Ordering::Equal)
        });
        let nelems = existing.len();

        let elemorder: f32 = if neighbor.is_none() {
            /* Put the new label at the end. */
            if nelems > 0 {
                existing[nelems - 1].enumsortorder + 1.0
            } else {
                1.0
            }
        } else {
            let neighbor = neighbor.unwrap();
            /* Locate the neighbor element. */
            let nbr_index = match existing.iter().position(|m| name_str(&m.enumlabel) == neighbor) {
                Some(i) => i,
                None => {
                    return Err(PgError::new(
                        ERROR,
                        format!("\"{neighbor}\" is not an existing enum label"),
                    )
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
                }
            };
            let nbr_sort = existing[nbr_index].enumsortorder;

            let other_nbr_index: isize = if newValIsAfter {
                nbr_index as isize + 1
            } else {
                nbr_index as isize - 1
            };

            if other_nbr_index < 0 {
                nbr_sort - 1.0
            } else if other_nbr_index as usize >= nelems {
                nbr_sort + 1.0
            } else {
                let other_sort = existing[other_nbr_index as usize].enumsortorder;
                /*
                 * The midpoint must be rounded to float4 precision (it already
                 * is — f32 arithmetic), else the equality checks are
                 * meaningless.
                 */
                let midpoint: f32 = (nbr_sort + other_sort) / 2.0;
                if midpoint == nbr_sort || midpoint == other_sort {
                    RenumberEnumType(mcx, &pg_enum, &existing)?;
                    /* Clean up and start over. */
                    continue 'restart;
                }
                midpoint
            }
        };

        /* Get a new OID for the new label. */
        let chosen_oid: Oid = if binary_upgrade_seams::is_binary_upgrade::call() {
            let next = binary_upgrade_seams::consume_next_pg_enum_oid::call();
            if !OidIsValid(next) {
                return Err(PgError::new(
                    ERROR,
                    "pg_enum OID value not set when in binary upgrade mode".to_string(),
                )
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            if neighbor.is_some() {
                return Err(PgError::new(
                    ERROR,
                    "ALTER TYPE ADD BEFORE/AFTER is incompatible with binary upgrade".to_string(),
                )
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            next
        } else {
            /*
             * Normal case: allocate an even-numbered Oid if it sorts correctly
             * relative to existing even-numbered labels; otherwise an odd Oid.
             */
            loop {
                let candidate =
                    indexing_seams::get_new_oid_with_index_pg_enum::call(&pg_enum)?;

                /*
                 * Detect whether it sorts correctly relative to existing
                 * even-numbered labels.
                 */
                let mut sorts_ok = true;
                for m in &existing {
                    let exists_oid = m.oid;
                    if exists_oid & 1 != 0 {
                        continue; /* ignore odd Oids */
                    }
                    if m.enumsortorder < elemorder {
                        /* should sort before */
                        if exists_oid >= candidate {
                            sorts_ok = false;
                            break;
                        }
                    } else {
                        /* should sort after */
                        if exists_oid <= candidate {
                            sorts_ok = false;
                            break;
                        }
                    }
                }

                if sorts_ok {
                    /* If it's even and sorts OK, we're done. */
                    if candidate & 1 == 0 {
                        break candidate;
                    }
                    /* Odd and sorts OK: loop back and try another OID. */
                } else {
                    /* Odd and sorts wrong: we're done. */
                    if candidate & 1 != 0 {
                        break candidate;
                    }
                    /* Even and sorts wrong: must reject; loop back. */
                }
            }
        };

        newOid = chosen_oid;
        newelemorder = elemorder;
        break 'restart;
    }

    /* Create the new pg_enum entry. */
    let row = PgEnumInsertRow {
        oid: newOid,
        enumtypid: enumTypeOid,
        enumsortorder: newelemorder,
        enumlabel: namestrcpy(newVal),
    };
    indexing_seams::catalog_tuple_insert_pg_enum::call(mcx, &pg_enum, &row)?;

    pg_enum.close(RowExclusiveLock)?;

    /*
     * If the enum type itself is uncommitted, we need not enter the value into
     * uncommitted_enum_values (only at the outermost tx level).
     */
    if xact_seams::get_current_transaction_nest_level::call() == 1
        && EnumTypeUncommitted(enumTypeOid)
    {
        return Ok(());
    }

    /* Set up the uncommitted values table and add the new value. */
    init_uncommitted_enum_values();
    UNCOMMITTED_ENUM_VALUES.with(|t| {
        if let Some(set) = t.borrow_mut().as_mut() {
            set.insert(newOid);
        }
    });

    Ok(())
}

/* ===========================================================================
 * RenameEnumLabel (pg_enum.c:606-683)
 * ========================================================================= */

/// RenameEnumLabel — rename a label in an enum set.
pub fn RenameEnumLabel(enumTypeOid: Oid, oldVal: &str, newVal: &str) -> PgResult<()> {
    /* check length of new label is ok */
    if newVal.len() > NAMEDATALEN - 1 {
        return Err(PgError::new(ERROR, format!("invalid enum label \"{newVal}\""))
            .with_sqlstate(ERRCODE_INVALID_NAME)
            .with_detail(format!("Labels must be {} bytes or less.", NAMEDATALEN - 1)));
    }

    /* Acquire a lock on the enum type, held until commit. */
    lmgr_seams::lock_database_object::call(TYPE_RELATION_ID, enumTypeOid, 0, ExclusiveLock)?.keep();

    let enum_ctx = MemoryContext::new("pg_enum");
    let mcx = enum_ctx.mcx();
    let pg_enum = table::table_open(mcx, EnumRelationId, RowExclusiveLock)?;

    /* Get the existing members. */
    let members = list_enum_members(mcx, &pg_enum, enumTypeOid)?;

    /*
     * Locate the element to rename and check whether the new label already
     * exists.
     */
    let mut old_member: Option<EnumMember> = None;
    let mut found_new = false;
    for m in &members {
        if name_str(&m.enumlabel) == oldVal {
            old_member = Some(m.clone());
        }
        if name_str(&m.enumlabel) == newVal {
            found_new = true;
        }
    }
    let old_member = match old_member {
        Some(m) => m,
        None => {
            return Err(PgError::new(
                ERROR,
                format!("\"{oldVal}\" is not an existing enum label"),
            )
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
    };
    if found_new {
        return Err(PgError::new(
            ERROR,
            format!("enum label \"{newVal}\" already exists"),
        )
        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
    }

    /* Update the pg_enum entry — namestrcpy(&en->enumlabel, newVal). */
    let row = PgEnumInsertRow {
        oid: old_member.oid,
        enumtypid: enumTypeOid,
        enumsortorder: old_member.enumsortorder,
        enumlabel: namestrcpy(newVal),
    };
    indexing_seams::catalog_tuple_update_pg_enum::call(mcx, &pg_enum, old_member.tid, &row)?;

    pg_enum.close(RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * RenumberEnumType (pg_enum.c:760-792)
 * ========================================================================= */

/// RenumberEnumType — renumber existing enum elements to have sort positions
/// 1..n. `existing` is the members sorted by `enumsortorder`.
fn RenumberEnumType<'mcx>(
    mcx: Mcx<'mcx>,
    pg_enum: &types_rel::Relation<'mcx>,
    existing: &[EnumMember],
) -> PgResult<()> {
    let nelems = existing.len();
    /*
     * We should only need to increase enumsortorders, never decrease. Work from
     * the end backwards to avoid uniqueness violations.
     */
    for i in (0..nelems).rev() {
        let m = &existing[i];
        let newsortorder = (i + 1) as f32;
        if m.enumsortorder != newsortorder {
            let row = PgEnumInsertRow {
                oid: m.oid,
                enumtypid: m.enumtypid,
                enumsortorder: newsortorder,
                enumlabel: m.enumlabel,
            };
            indexing_seams::catalog_tuple_update_pg_enum::call(mcx, pg_enum, m.tid, &row)?;
        }
    }

    /* Make the updates visible. */
    xact_seams::command_counter_increment::call()?;

    Ok(())
}

/* ===========================================================================
 * EstimateUncommittedEnumsSpace / SerializeUncommittedEnums /
 * RestoreUncommittedEnums (pg_enum.c:812-906) — parallel-worker DSM transfer.
 * ========================================================================= */

/// EstimateUncommittedEnumsSpace — the DSM bytes needed to serialize the two
/// uncommitted-enum tables plus two `InvalidOid` terminators.
pub fn EstimateUncommittedEnumsSpace() -> Size {
    let mut entries: usize = 0;
    entries += UNCOMMITTED_ENUM_TYPES.with(|t| t.borrow().as_ref().map_or(0, |s| s.len()));
    entries += UNCOMMITTED_ENUM_VALUES.with(|t| t.borrow().as_ref().map_or(0, |s| s.len()));
    /* Add two for the terminators. */
    (core::mem::size_of::<Oid>() * (entries + 2)) as Size
}

/// SerializeUncommittedEnums — write the OIDs of both tables (each terminated by
/// `InvalidOid`) into the DSM chunk at `space`. `len` must equal
/// `EstimateUncommittedEnumsSpace()`.
///
/// # Safety
/// `space` must point to at least `len` writable bytes (the parallel.c
/// `shm_toc_allocate` chunk). This mirrors the C raw-pointer serialize.
pub fn SerializeUncommittedEnums(space: usize, len: Size) {
    debug_assert_eq!(len, EstimateUncommittedEnumsSpace());

    let mut p = space as *mut Oid;
    // SAFETY: parallel.c reserved `len` bytes via EstimateUncommittedEnumsSpace.
    unsafe {
        UNCOMMITTED_ENUM_TYPES.with(|t| {
            if let Some(set) = t.borrow().as_ref() {
                for &value in set {
                    core::ptr::write(p, value);
                    p = p.add(1);
                }
            }
        });
        /* terminator */
        core::ptr::write(p, InvalidOid);
        p = p.add(1);

        UNCOMMITTED_ENUM_VALUES.with(|t| {
            if let Some(set) = t.borrow().as_ref() {
                for &value in set {
                    core::ptr::write(p, value);
                    p = p.add(1);
                }
            }
        });
        /* terminator */
        core::ptr::write(p, InvalidOid);
        p = p.add(1);

        debug_assert_eq!(p as usize, space + len as usize);
    }
}

/// RestoreUncommittedEnums — read both terminated OID lists back from the DSM
/// chunk at `space` into fresh backend-local tables.
///
/// # Safety
/// `space` must point to a region written by [`SerializeUncommittedEnums`].
pub fn RestoreUncommittedEnums(space: usize) {
    debug_assert!(UNCOMMITTED_ENUM_TYPES.with(|t| t.borrow().is_none()));
    debug_assert!(UNCOMMITTED_ENUM_VALUES.with(|t| t.borrow().is_none()));

    let mut p = space as *const Oid;
    // SAFETY: `space` was written by SerializeUncommittedEnums (terminated lists).
    unsafe {
        if OidIsValid(core::ptr::read(p)) {
            init_uncommitted_enum_types();
            UNCOMMITTED_ENUM_TYPES.with(|t| {
                let mut b = t.borrow_mut();
                let set = b.as_mut().unwrap();
                loop {
                    let v = core::ptr::read(p);
                    if !OidIsValid(v) {
                        break;
                    }
                    set.insert(v);
                    p = p.add(1);
                }
            });
        }
        /* skip the types terminator */
        p = p.add(1);
        if OidIsValid(core::ptr::read(p)) {
            init_uncommitted_enum_values();
            UNCOMMITTED_ENUM_VALUES.with(|t| {
                let mut b = t.borrow_mut();
                let set = b.as_mut().unwrap();
                loop {
                    let v = core::ptr::read(p);
                    if !OidIsValid(v) {
                        break;
                    }
                    set.insert(v);
                    p = p.add(1);
                }
            });
        }
    }
}

/* ===========================================================================
 * Inward-seam adapters + install
 * ========================================================================= */

/// `scan_enum_members` seam (typcache.c `load_enum_cache_data`): emit
/// `(enum_oid, enumsortorder)` for each member of `enum_type_id` in catalog
/// order (the typcache sorts).
fn scan_enum_members_seam(
    enum_type_id: Oid,
    emit: &mut dyn FnMut(Oid, f32),
) -> PgResult<()> {
    let ctx = MemoryContext::new("scan_enum_members");
    let mcx = ctx.mcx();
    let pg_enum = table::table_open(mcx, EnumRelationId, types_storage::lock::AccessShareLock)?;
    let members = list_enum_members(mcx, &pg_enum, enum_type_id)?;
    pg_enum.close(types_storage::lock::AccessShareLock)?;
    for m in &members {
        emit(m.oid, m.enumsortorder);
    }
    Ok(())
}

/// Install this crate's implementations into its seam crates.
pub fn init_seams() {
    use backend_access_transam_parallel_rt_seams as rt;
    use backend_catalog_pg_enum_seams as seams;

    seams::at_eoxact_enum::set(AtEOXact_Enum);
    seams::scan_enum_members::set(scan_enum_members_seam);

    rt::estimate_uncommitted_enums_space::set(|| Ok(EstimateUncommittedEnumsSpace()));
    rt::serialize_uncommitted_enums::set(|space, len| {
        SerializeUncommittedEnums(space, len);
        Ok(())
    });
    rt::restore_uncommitted_enums::set(|space| {
        RestoreUncommittedEnums(space);
        Ok(())
    });
}
