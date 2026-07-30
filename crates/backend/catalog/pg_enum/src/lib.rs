// pg_enum.c. LOUD divergence: parallel-DSM serialize/restore of the
// uncommitted tables.
#![allow(non_snake_case, non_upper_case_globals)]

use core::cell::{Cell, RefCell};

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid, OidIsValid, NAMEDATALEN, TYPE_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INVALID_NAME,
    ERRCODE_INVALID_PARAMETER_VALUE, ERROR, NOTICE,
};
use types_rel::{AccessShareLock, ExclusiveLock, Relation, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_scan::sdir::ScanDirection;
use types_tuple::{HeapTupleData, ItemPointerData, NameData};

use cache_syscache::{
    ReleaseSysCache, ReleaseSysCacheList, SearchSysCache2, SearchSysCacheList1, SysCacheKey,
    ENUMTYPOIDNAME,
};

pub const EnumRelationId: Oid = 3501;
pub const EnumOidIndexId: Oid = 3502;
pub const EnumTypIdLabelIndexId: Oid = 3503;
pub const EnumTypIdSortOrderIndexId: Oid = 3534;

pub const Anum_pg_enum_oid: AttrNumber = 1;
pub const Anum_pg_enum_enumtypid: AttrNumber = 2;
pub const Anum_pg_enum_enumsortorder: AttrNumber = 3;
pub const Anum_pg_enum_enumlabel: AttrNumber = 4;
const Natts_pg_enum: usize = 4;

// C's two TopTransactionContext HTABs; None == the C NULL table pointer.
// Tiny per-tx sets: linear membership over a retained backend-life arena
// replaces the C hash (cleared, not freed, at EOX).
struct Uncommitted {
    mcx: Option<Mcx<'static>>,
    types: Option<PgVec<'static, Oid>>,
    values: Option<PgVec<'static, Oid>>,
}

impl Uncommitted {
    fn mcx(&mut self) -> Mcx<'static> {
        *self
            .mcx
            .get_or_insert_with(|| mcx::session_root("UncommittedEnums").mcx())
    }
}

thread_local! {
    static UNCOMMITTED: RefCell<core::mem::ManuallyDrop<Uncommitted>> = const {
        RefCell::new(core::mem::ManuallyDrop::new(Uncommitted {
            mcx: None,
            types: None,
            values: None,
        }))
    };
}

pub fn EnumUncommitted(enum_id: Oid) -> bool {
    UNCOMMITTED.with(|u| u.borrow().values.as_ref().is_some_and(|v| v.contains(&enum_id)))
}

pub fn HasUncommittedEnums() -> bool {
    UNCOMMITTED.with(|u| {
        let u = u.borrow();
        u.types.as_ref().is_some_and(|v| !v.is_empty())
            || u.values.as_ref().is_some_and(|v| !v.is_empty())
    })
}

fn EnumTypeUncommitted(typ_id: Oid) -> bool {
    UNCOMMITTED.with(|u| u.borrow().types.as_ref().is_some_and(|v| v.contains(&typ_id)))
}

pub fn AtEOXact_Enum() {
    UNCOMMITTED.with(|u| {
        let mut u = u.borrow_mut();
        u.types = None;
        u.values = None;
    });
}

fn oid_key(attno: AttrNumber, value: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(value);
    key
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_label(lab: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("invalid enum label \"{lab}\""))
            .with_sqlstate(ERRCODE_INVALID_NAME)
            .with_detail(format!("Labels must be {} bytes or less.", NAMEDATALEN - 1)),
    )
}

fn form_and_insert<'mcx>(
    mcx: Mcx<'mcx>,
    pg_enum: &Relation<'mcx>,
    oid: Oid,
    enumtypid: Oid,
    sortorder: f32,
    label: &NameData,
) -> PgResult<()> {
    let mut values = [Datum::null(); Natts_pg_enum];
    let nulls = [false; Natts_pg_enum];
    values[0] = Datum::from_oid(oid);
    values[1] = Datum::from_oid(enumtypid);
    values[2] = Datum::from_f32(sortorder);
    values[3] = Datum::from_usize(label.data.as_ptr() as usize);
    let mut tup = heaptuple::heap_form_tuple(mcx, pg_enum.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, pg_enum, &mut tup)
}

pub fn EnumValuesCreate<'mcx>(mcx: Mcx<'mcx>, enumTypeOid: Oid, vals: &[&str]) -> PgResult<()> {
    if xact::GetCurrentTransactionNestLevel() == 1 {
        UNCOMMITTED.with(|u| {
            let mut u = u.borrow_mut();
            let smcx = u.mcx();
            let t = u.types.get_or_insert_with(|| PgVec::new_in(smcx));
            if !t.contains(&enumTypeOid) {
                t.push(enumTypeOid);
            }
        });
    }

    let num_elems = vals.len();
    let pg_enum = table::table_open(mcx, EnumRelationId, RowExclusiveLock)?;

    // Even-numbered OIDs mark labels the comparison fast path may compare
    // directly.
    let mut oids: PgVec<'mcx, Oid> = PgVec::with_capacity_in(num_elems, mcx);
    for _ in 0..num_elems {
        let new_oid = loop {
            let o = catalog::GetNewOidWithIndex(mcx, &pg_enum, EnumOidIndexId, Anum_pg_enum_oid)?;
            if o & 1 == 0 {
                break o;
            }
        };
        oids.push(new_oid);
    }
    oids.sort_unstable();

    // C divergence: C batches through CatalogTuplesMultiInsertWithInfo;
    // per-row CatalogTupleInsert yields identical rows (only the WAL record
    // shape differs).
    for (elemno, lab) in vals.iter().enumerate() {
        if lab.len() > NAMEDATALEN as usize - 1 {
            return Err(invalid_label(lab));
        }
        let mut label = NameData::default();
        label.namestrcpy(lab);
        form_and_insert(mcx, &pg_enum, oids[elemno], enumTypeOid, (elemno + 1) as f32, &label)?;
    }

    pg_enum.close(RowExclusiveLock)
}

pub fn EnumValuesDelete<'mcx>(mcx: Mcx<'mcx>, enumTypeOid: Oid) -> PgResult<()> {
    let pg_enum = table::table_open(mcx, EnumRelationId, RowExclusiveLock)?;
    let key = [oid_key(Anum_pg_enum_enumtypid, enumTypeOid)];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_enum, EnumTypIdLabelIndexId, true, None, &key)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&pg_enum, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    pg_enum.close(RowExclusiveLock)
}

struct EnumMember {
    oid: Oid,
    enumtypid: Oid,
    enumsortorder: f32,
    enumlabel: NameData,
    tid: ItemPointerData,
}

fn decode_member(tup: &HeapTupleData<'_>, descr: &types_tuple::TupleDescData<'_>) -> EnumMember {
    let mut isnull = false;
    // SAFETY (each): fixed NOT NULL pg_enum columns of the declared types.
    let get = |attno: AttrNumber, isnull: &mut bool| unsafe {
        types_tuple::heap_getattr(tup, attno as i32, descr, isnull)
    };
    let mut enumlabel = NameData::default();
    let label_ptr = get(Anum_pg_enum_enumlabel, &mut isnull).as_usize() as *const u8;
    debug_assert!(!isnull, "pg_enum.enumlabel NOT NULL invariant");
    // SAFETY: name-column datum points at NAMEDATALEN bytes in the tuple image.
    unsafe {
        core::ptr::copy_nonoverlapping(label_ptr, enumlabel.data.as_mut_ptr(), NAMEDATALEN as usize)
    };
    EnumMember {
        oid: get(Anum_pg_enum_oid, &mut isnull).as_oid(),
        enumtypid: get(Anum_pg_enum_enumtypid, &mut isnull).as_oid(),
        enumsortorder: get(Anum_pg_enum_enumsortorder, &mut isnull).as_f32(),
        enumlabel,
        tid: tup.t_self,
    }
}

fn label_str(name: &NameData) -> &str {
    core::str::from_utf8(name.name_str()).unwrap_or("")
}

fn list_enum_members<'mcx>(
    mcx: Mcx<'mcx>,
    pg_enum: &Relation<'mcx>,
    enumTypeOid: Oid,
) -> PgResult<PgVec<'mcx, EnumMember>> {
    let list =
        SearchSysCacheList1(ENUMTYPOIDNAME, SysCacheKey::Value(Datum::from_oid(enumTypeOid)))?;
    let n = list.n_members() as usize;
    let mut out: PgVec<'mcx, EnumMember> = PgVec::with_capacity_in(n, mcx);
    for i in 0..n {
        let m = list.member(i);
        out.push(decode_member(&m.tuple(), pg_enum.descr()));
    }
    ReleaseSysCacheList(list);
    Ok(out)
}

pub fn AddEnumLabel<'mcx>(
    mcx: Mcx<'mcx>,
    enumTypeOid: Oid,
    newVal: &str,
    neighbor: Option<&str>,
    newValIsAfter: bool,
    skipIfExists: bool,
) -> PgResult<()> {
    if newVal.len() > NAMEDATALEN as usize - 1 {
        return Err(invalid_label(newVal));
    }

    // Held until commit: serializes concurrent modifications of one enum.
    lmgr::LockDatabaseObject(TYPE_RELATION_ID, enumTypeOid, 0, ExclusiveLock)?;

    let dup = SearchSysCache2(
        ENUMTYPOIDNAME,
        SysCacheKey::Value(Datum::from_oid(enumTypeOid)),
        SysCacheKey::Str(newVal),
    )?;
    if let Some(tup) = dup {
        ReleaseSysCache(tup);
        if skipIfExists {
            elog_seams::ereport::call(
                PgError::new(NOTICE, format!("enum label \"{newVal}\" already exists, skipping"))
                    .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
            )?;
            return Ok(());
        }
        return Err(label_exists(newVal));
    }

    let pg_enum = table::table_open(mcx, EnumRelationId, RowExclusiveLock)?;

    let (newOid, newelemorder) = 'restart: loop {
        let mut existing = list_enum_members(mcx, &pg_enum, enumTypeOid)?;
        existing.sort_by(|a, b| {
            a.enumsortorder.partial_cmp(&b.enumsortorder).unwrap_or(core::cmp::Ordering::Equal)
        });
        let nelems = existing.len();

        let newelemorder: f32 = match neighbor {
            None => {
                if nelems > 0 {
                    existing[nelems - 1].enumsortorder + 1.0
                } else {
                    1.0
                }
            }
            Some(neighbor) => {
                let Some(nbr_index) =
                    existing.iter().position(|m| label_str(&m.enumlabel) == neighbor)
                else {
                    return Err(not_existing_label(neighbor));
                };
                let nbr_order = existing[nbr_index].enumsortorder;
                let other_nbr_index =
                    if newValIsAfter { nbr_index as i64 + 1 } else { nbr_index as i64 - 1 };
                if other_nbr_index < 0 {
                    nbr_order - 1.0
                } else if other_nbr_index >= nelems as i64 {
                    nbr_order + 1.0
                } else {
                    let other_order = existing[other_nbr_index as usize].enumsortorder;
                    // f32 arithmetic rounds to float4 precision, so these
                    // equality probes are exactly C's volatile-midpoint test.
                    let midpoint: f32 = (nbr_order + other_order) / 2.0;
                    if midpoint == nbr_order || midpoint == other_order {
                        RenumberEnumType(mcx, &pg_enum, &existing)?;
                        continue 'restart;
                    }
                    midpoint
                }
            }
        };

        let newOid = if init_small::globals::IsBinaryUpgrade() {
            let oid = take_next_pg_enum_oid().ok_or_else(oid_not_set)?;
            if neighbor.is_some() {
                return Err(binary_upgrade_incompatible_neighbor());
            }
            oid
        } else {
            // Prefer an even OID when it sorts correctly against existing even
            // OIDs; otherwise the value must carry an odd OID.
            loop {
                let candidate =
                    catalog::GetNewOidWithIndex(mcx, &pg_enum, EnumOidIndexId, Anum_pg_enum_oid)?;
                let mut sorts_ok = true;
                for m in existing.iter() {
                    if m.oid & 1 != 0 {
                        continue;
                    }
                    if m.enumsortorder < newelemorder {
                        if m.oid >= candidate {
                            sorts_ok = false;
                            break;
                        }
                    } else if m.oid <= candidate {
                        sorts_ok = false;
                        break;
                    }
                }
                if sorts_ok {
                    if candidate & 1 == 0 {
                        break candidate;
                    }
                } else if candidate & 1 != 0 {
                    break candidate;
                }
            }
        };

        break 'restart (newOid, newelemorder);
    };

    let mut label = NameData::default();
    label.namestrcpy(newVal);
    form_and_insert(mcx, &pg_enum, newOid, enumTypeOid, newelemorder, &label)?;

    pg_enum.close(RowExclusiveLock)?;

    if xact::GetCurrentTransactionNestLevel() == 1 && EnumTypeUncommitted(enumTypeOid) {
        return Ok(());
    }

    UNCOMMITTED.with(|u| {
        let mut u = u.borrow_mut();
        let smcx = u.mcx();
        let v = u.values.get_or_insert_with(|| PgVec::new_in(smcx));
        if !v.contains(&newOid) {
            v.push(newOid);
        }
    });
    Ok(())
}

pub fn RenameEnumLabel<'mcx>(
    mcx: Mcx<'mcx>,
    enumTypeOid: Oid,
    oldVal: &str,
    newVal: &str,
) -> PgResult<()> {
    if newVal.len() > NAMEDATALEN as usize - 1 {
        return Err(invalid_label(newVal));
    }

    lmgr::LockDatabaseObject(TYPE_RELATION_ID, enumTypeOid, 0, ExclusiveLock)?;

    let pg_enum = table::table_open(mcx, EnumRelationId, RowExclusiveLock)?;
    let members = list_enum_members(mcx, &pg_enum, enumTypeOid)?;

    let mut old_member: Option<&EnumMember> = None;
    let mut found_new = false;
    for m in members.iter() {
        if label_str(&m.enumlabel) == oldVal {
            old_member = Some(m);
        }
        if label_str(&m.enumlabel) == newVal {
            found_new = true;
        }
    }
    let Some(old) = old_member else {
        return Err(not_existing_label(oldVal));
    };
    if found_new {
        return Err(label_exists(newVal));
    }

    let mut label = NameData::default();
    label.namestrcpy(newVal);
    update_member(mcx, &pg_enum, old, old.enumsortorder, &label)?;

    pg_enum.close(RowExclusiveLock)
}

fn update_member<'mcx>(
    mcx: Mcx<'mcx>,
    pg_enum: &Relation<'mcx>,
    m: &EnumMember,
    sortorder: f32,
    label: &NameData,
) -> PgResult<()> {
    let mut values = [Datum::null(); Natts_pg_enum];
    let nulls = [false; Natts_pg_enum];
    values[0] = Datum::from_oid(m.oid);
    values[1] = Datum::from_oid(m.enumtypid);
    values[2] = Datum::from_f32(sortorder);
    values[3] = Datum::from_usize(label.data.as_ptr() as usize);
    let mut tup = heaptuple::heap_form_tuple(mcx, pg_enum.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleUpdate(mcx, pg_enum, &m.tid, &mut tup)
}

// Renumber existing elements to sort positions 1..n; only ever increases
// orders, so walk backwards to dodge uniqueness violations.
fn RenumberEnumType<'mcx>(
    mcx: Mcx<'mcx>,
    pg_enum: &Relation<'mcx>,
    existing: &[EnumMember],
) -> PgResult<()> {
    for i in (0..existing.len()).rev() {
        let m = &existing[i];
        let newsortorder = (i + 1) as f32;
        if m.enumsortorder != newsortorder {
            update_member(mcx, pg_enum, m, newsortorder, &m.enumlabel)?;
        }
    }
    xact::CommandCounterIncrement()
}

// enum.c's ordered EnumTypIdSortOrderIndexId scan (enum_endpoint /
// enum_range_internal); syscache is off-limits there, see RenumberEnumType.
// Header xmin facts ride along for check_safe_enum_use.
pub fn scan_enum_typid_sorted<'mcx>(
    mcx: Mcx<'mcx>,
    enumtypoid: Oid,
    backward: bool,
    limit_one: bool,
) -> PgResult<PgVec<'mcx, pg_enum_seams::EnumSortedRow>> {
    let direction =
        if backward { ScanDirection::BackwardScanDirection } else { ScanDirection::ForwardScanDirection };
    let key = [oid_key(Anum_pg_enum_enumtypid, enumtypoid)];
    let enum_rel = table::table_open(mcx, EnumRelationId, AccessShareLock)?;
    let enum_idx = indexam::index_open(mcx, EnumTypIdSortOrderIndexId, AccessShareLock)?;
    let mut scan = genam::systable_beginscan_ordered(mcx, &enum_rel, &enum_idx, None, &key)?;

    let mut out: PgVec<'mcx, pg_enum_seams::EnumSortedRow> = PgVec::new_in(mcx);
    loop {
        let Some(tup) = genam::systable_getnext_ordered(mcx, &mut scan, direction)? else {
            break;
        };
        let hdr_xmin;
        let hdr_committed;
        {
            let hdr = tup.t_data();
            hdr_xmin = hdr.xmin();
            hdr_committed = hdr.xmin_committed();
        }
        let m = decode_member(tup, enum_rel.descr());
        out.push(pg_enum_seams::EnumSortedRow {
            oid: m.oid,
            enumtypid: m.enumtypid,
            enumlabel: m.enumlabel,
            xmin: hdr_xmin,
            xmin_committed: hdr_committed,
        });
        if limit_one {
            break;
        }
    }
    genam::systable_endscan_ordered(mcx, scan)?;
    enum_idx.close(AccessShareLock)?;
    enum_rel.close(AccessShareLock)?;
    Ok(out)
}

// typcache load_enum_cache_data's plain EnumTypIdLabelIndexId scan.
pub fn scan_enum_members<'mcx>(
    mcx: Mcx<'mcx>,
    enumTypeOid: Oid,
) -> PgResult<PgVec<'mcx, (Oid, f32)>> {
    let pg_enum = table::table_open(mcx, EnumRelationId, AccessShareLock)?;
    let key = [oid_key(Anum_pg_enum_enumtypid, enumTypeOid)];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_enum, EnumTypIdLabelIndexId, true, None, &key)?;
    let mut out: PgVec<'mcx, (Oid, f32)> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let m = decode_member(tup, pg_enum.descr());
        out.push((m.oid, m.enumsortorder));
    }
    genam::systable_endscan(mcx, scan)?;
    pg_enum.close(AccessShareLock)?;
    Ok(out)
}

// binary_upgrade_next_pg_enum_oid (pg_upgrade_support.c): set-once,
// consume-once override for AddEnumLabel's OID search (pg_enum.c:458-477).
thread_local! {
    static NEXT_PG_ENUM_OID: Cell<Oid> = const { Cell::new(InvalidOid) };
}

pub fn SetNextPgEnumOid(oid: Oid) {
    NEXT_PG_ENUM_OID.set(oid);
}

fn take_next_pg_enum_oid() -> Option<Oid> {
    let oid = NEXT_PG_ENUM_OID.get();
    if OidIsValid(oid) {
        NEXT_PG_ENUM_OID.set(InvalidOid);
        Some(oid)
    } else {
        None
    }
}

fn oid_not_set() -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "pg_enum OID value not set when in binary upgrade mode".to_string())
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

fn binary_upgrade_incompatible_neighbor() -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            "ALTER TYPE ADD BEFORE/AFTER is incompatible with binary upgrade".to_string(),
        )
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn label_exists(label: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("enum label \"{label}\" already exists"))
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_existing_label(label: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("\"{label}\" is not an existing enum label"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

pub fn init_seams() {
    pg_enum_seams::at_eoxact_enum::set(AtEOXact_Enum);
    pg_enum_seams::enum_uncommitted::set(EnumUncommitted);
    pg_enum_seams::scan_enum_members::set(scan_enum_members);
    pg_enum_seams::scan_enum_typid_sorted::set(scan_enum_typid_sorted);
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_core::{FLOAT4OID, NAMEOID, OIDOID};
    use types_tuple::{FormData_pg_attribute, CompactAttribute, TupleDescData};
    use types_tuple::{TYPALIGN_CHAR, TYPALIGN_INT};

    fn attr(num: i16, typid: Oid, len: i16, byval: bool, align: i8) -> FormData_pg_attribute {
        let mut attname = NameData::default();
        attname.namestrcpy(&format!("a{num}"));
        FormData_pg_attribute {
            attname,
            attnum: num,
            atttypid: typid,
            atttypmod: -1,
            attlen: len,
            attbyval: byval,
            attalign: align,
            attnotnull: true,
            ..Default::default()
        }
    }

    fn pg_enum_desc(mcx: Mcx<'_>) -> TupleDescData<'_> {
        let atts = [
            attr(1, OIDOID, 4, true, TYPALIGN_INT),
            attr(2, OIDOID, 4, true, TYPALIGN_INT),
            attr(3, FLOAT4OID, 4, true, TYPALIGN_INT),
            attr(4, NAMEOID, NAMEDATALEN as i16, false, TYPALIGN_CHAR),
        ];
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        for att in atts {
            compact.push(CompactAttribute::populate_from(&att));
            attrs.push(att);
        }
        TupleDescData {
            natts: 4,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        }
    }

    // Miri gate for decode_member's raw NAMEDATALEN copy: the formed tuple
    // image is exactly-sized, so an over-read past the name column faults.
    #[test]
    fn decode_member_roundtrip_tight_image() {
        let ctx = mcx::MemoryContext::new("pg_enum-test");
        let mcx = ctx.mcx();
        let descr = pg_enum_desc(mcx);
        for label in ["a", &"x".repeat(NAMEDATALEN as usize - 1)] {
            let mut name = NameData::default();
            name.namestrcpy(label);
            let values = [
                Datum::from_oid(90101),
                Datum::from_oid(90000),
                Datum::from_f32(2.5),
                Datum::from_usize(name.data.as_ptr() as usize),
            ];
            let nulls = [false; 4];
            let tup = heaptuple::heap_form_tuple(mcx, &descr, &values, &nulls).unwrap();
            let m = decode_member(&tup, &descr);
            assert_eq!(m.oid, 90101);
            assert_eq!(m.enumtypid, 90000);
            assert_eq!(m.enumsortorder, 2.5);
            assert_eq!(label_str(&m.enumlabel), label);
        }
    }

    #[test]
    fn next_pg_enum_oid_set_take_once() {
        assert_eq!(take_next_pg_enum_oid(), None);
        SetNextPgEnumOid(123456);
        assert_eq!(take_next_pg_enum_oid(), Some(123456));
        assert_eq!(take_next_pg_enum_oid(), None);
    }
}
