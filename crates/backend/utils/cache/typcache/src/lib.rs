#![allow(non_snake_case)]

pub mod domain;
mod enum_cmp;
mod invalidate;
#[cfg(test)]
mod tests;

use core::cell::{Cell, RefCell, RefMut};
use core::mem::ManuallyDrop;
use std::rc::Rc;

use lsyscache::{
    get_base_element_type, get_opclass_family, get_opclass_input_type, get_opcode,
    get_opfamily_member, get_opfamily_proc, BTEqualStrategyNumber, BTGreaterStrategyNumber,
    BTLessStrategyNumber, HTEqualStrategyNumber, HASH_AM_OID, TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN,
    TYPTYPE_MULTIRANGE, TYPTYPE_RANGE,
};
use mcx::{Mcx, MemoryContext, PgHashMap, PgVec};
use syscache_seams::PgTypeTypcacheShape;
use types_core::{InvalidOid, Oid, BTREE_AM_OID};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT};
use types_fmgr::FmgrInfo;

pub use domain::{
    DomConstraintType, DomainConstraintCache, DomainConstraintRef, DomainConstraintState,
    DomainHasConstraints,
};
pub use enum_cmp::compare_values_of_enum;
pub use invalidate::{AtEOSubXact_TypeCache, AtEOXact_TypeCache};

// typcache.h
pub const TYPECACHE_EQ_OPR: i32 = 0x00001;
pub const TYPECACHE_LT_OPR: i32 = 0x00002;
pub const TYPECACHE_GT_OPR: i32 = 0x00004;
pub const TYPECACHE_CMP_PROC: i32 = 0x00008;
pub const TYPECACHE_HASH_PROC: i32 = 0x00010;
pub const TYPECACHE_EQ_OPR_FINFO: i32 = 0x00020;
pub const TYPECACHE_CMP_PROC_FINFO: i32 = 0x00040;
pub const TYPECACHE_HASH_PROC_FINFO: i32 = 0x00080;
pub const TYPECACHE_TUPDESC: i32 = 0x00100;
pub const TYPECACHE_BTREE_OPFAMILY: i32 = 0x00200;
pub const TYPECACHE_HASH_OPFAMILY: i32 = 0x00400;
pub const TYPECACHE_RANGE_INFO: i32 = 0x00800;
pub const TYPECACHE_DOMAIN_BASE_INFO: i32 = 0x01000;
pub const TYPECACHE_DOMAIN_CONSTR_INFO: i32 = 0x02000;
pub const TYPECACHE_HASH_EXTENDED_PROC: i32 = 0x04000;
pub const TYPECACHE_HASH_EXTENDED_PROC_FINFO: i32 = 0x08000;
pub const TYPECACHE_MULTIRANGE_INFO: i32 = 0x10000;

// typcache.c TCFLAGS_*
pub(crate) const TCFLAGS_HAVE_PG_TYPE_DATA: i32 = 0x000001;
const TCFLAGS_CHECKED_BTREE_OPCLASS: i32 = 0x000002;
const TCFLAGS_CHECKED_HASH_OPCLASS: i32 = 0x000004;
const TCFLAGS_CHECKED_EQ_OPR: i32 = 0x000008;
const TCFLAGS_CHECKED_LT_OPR: i32 = 0x000010;
const TCFLAGS_CHECKED_GT_OPR: i32 = 0x000020;
const TCFLAGS_CHECKED_CMP_PROC: i32 = 0x000040;
const TCFLAGS_CHECKED_HASH_PROC: i32 = 0x000080;
const TCFLAGS_CHECKED_HASH_EXTENDED_PROC: i32 = 0x000100;
const TCFLAGS_CHECKED_ELEM_PROPERTIES: i32 = 0x000200;
const TCFLAGS_HAVE_ELEM_EQUALITY: i32 = 0x000400;
const TCFLAGS_HAVE_ELEM_COMPARE: i32 = 0x000800;
const TCFLAGS_HAVE_ELEM_HASHING: i32 = 0x001000;
const TCFLAGS_HAVE_ELEM_EXTENDED_HASHING: i32 = 0x002000;
const TCFLAGS_CHECKED_FIELD_PROPERTIES: i32 = 0x004000;
const TCFLAGS_HAVE_FIELD_EQUALITY: i32 = 0x008000;
const TCFLAGS_HAVE_FIELD_COMPARE: i32 = 0x010000;
const TCFLAGS_HAVE_FIELD_HASHING: i32 = 0x020000;
const TCFLAGS_HAVE_FIELD_EXTENDED_HASHING: i32 = 0x040000;
pub(crate) const TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS: i32 = 0x080000;
pub(crate) const TCFLAGS_DOMAIN_BASE_IS_COMPOSITE: i32 = 0x100000;
pub(crate) const TCFLAGS_OPERATOR_FLAGS: i32 = !(TCFLAGS_HAVE_PG_TYPE_DATA
    | TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS
    | TCFLAGS_DOMAIN_BASE_IS_COMPOSITE);

// Private ready bit: pg_type scalars loaded (no public TYPECACHE flag asks for
// them, but flags==0 lookups must reload after a pg_type inval).
const READY_PG_TYPE_DATA: i32 = 0x4000_0000;

// syscache_ids.h
const TYPEOID: i32 = 82;
const CLAOID: i32 = 14;
const CONSTROID: i32 = 19;

// nbtree.h / hash.h support proc numbers
const BTORDER_PROC: i16 = 1;
const HASHSTANDARD_PROC: i16 = 1;
const HASHEXTENDED_PROC: i16 = 2;

// pg_operator.dat / pg_proc.dat
const ARRAY_EQ_OP: Oid = 1070;
const ARRAY_LT_OP: Oid = 1072;
const ARRAY_GT_OP: Oid = 1073;
const RECORD_EQ_OP: Oid = 2988;
const RECORD_LT_OP: Oid = 2990;
const RECORD_GT_OP: Oid = 2991;
const F_BTARRAYCMP: Oid = 382;
const F_BTRECORDCMP: Oid = 2987;
const F_HASH_ARRAY: Oid = 626;
const F_HASH_ARRAY_EXTENDED: Oid = 782;
const F_HASH_RECORD: Oid = 6192;
const F_HASH_RECORD_EXTENDED: Oid = 6193;
const F_HASH_RANGE: Oid = 3902;
const F_HASH_RANGE_EXTENDED: Oid = 3417;
const F_HASH_MULTIRANGE: Oid = 4278;
const F_HASH_MULTIRANGE_EXTENDED: Oid = 4279;

// Entries are handed out as Rc pins (rule 2: C returns a stable pointer into
// TypeCacheHash that callers keep for the backend's life) and lazily filled in
// place through Cells, exactly like C mutating the entry callers point at.
pub struct TypeCacheEntry {
    pub type_id: Oid,
    pub type_id_hash: u32,
    typlen: Cell<i16>,
    typbyval: Cell<bool>,
    typalign: Cell<i8>,
    typstorage: Cell<i8>,
    typtype: Cell<i8>,
    typrelid: Cell<Oid>,
    typsubscript: Cell<Oid>,
    typelem: Cell<Oid>,
    typarray: Cell<Oid>,
    typcollation: Cell<Oid>,
    btree_opf: Cell<Oid>,
    btree_opintype: Cell<Oid>,
    hash_opf: Cell<Oid>,
    hash_opintype: Cell<Oid>,
    eq_opr: Cell<Oid>,
    lt_opr: Cell<Oid>,
    gt_opr: Cell<Oid>,
    cmp_proc: Cell<Oid>,
    hash_proc: Cell<Oid>,
    hash_extended_proc: Cell<Oid>,
    eq_opr_finfo: RefCell<FmgrInfo>,
    cmp_proc_finfo: RefCell<FmgrInfo>,
    hash_proc_finfo: RefCell<FmgrInfo>,
    hash_extended_proc_finfo: RefCell<FmgrInfo>,
    rng_collation: Cell<Oid>,
    rng_opfamily: Cell<Oid>,
    rng_elemtype: RefCell<Option<Rc<TypeCacheEntry>>>,
    rng_cmp_proc_finfo: RefCell<FmgrInfo>,
    rng_canonical_finfo: RefCell<FmgrInfo>,
    rng_subdiff_finfo: RefCell<FmgrInfo>,
    rngtype: RefCell<Option<Rc<TypeCacheEntry>>>,
    domain_base_type: Cell<Oid>,
    domain_base_typmod: Cell<i32>,
    domain_data: Cell<Option<&'static domain::DomainConstraintCache>>,
    flags: Cell<i32>,
    ready: Cell<i32>,
    next_domain: Cell<Oid>,
    enum_data: RefCell<Option<enum_cmp::EnumData>>,
}

impl core::fmt::Debug for TypeCacheEntry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TypeCacheEntry").field("type_id", &self.type_id).finish_non_exhaustive()
    }
}

macro_rules! cell_getters {
    ($($name:ident: $ty:ty),+ $(,)?) => {
        $(
            #[inline]
            pub fn $name(&self) -> $ty {
                self.$name.get()
            }
        )+
    };
}

impl TypeCacheEntry {
    fn new(type_id: Oid, type_id_hash: u32) -> Self {
        TypeCacheEntry {
            type_id,
            type_id_hash,
            typlen: Cell::new(0),
            typbyval: Cell::new(false),
            typalign: Cell::new(0),
            typstorage: Cell::new(0),
            typtype: Cell::new(0),
            typrelid: Cell::new(InvalidOid),
            typsubscript: Cell::new(InvalidOid),
            typelem: Cell::new(InvalidOid),
            typarray: Cell::new(InvalidOid),
            typcollation: Cell::new(InvalidOid),
            btree_opf: Cell::new(InvalidOid),
            btree_opintype: Cell::new(InvalidOid),
            hash_opf: Cell::new(InvalidOid),
            hash_opintype: Cell::new(InvalidOid),
            eq_opr: Cell::new(InvalidOid),
            lt_opr: Cell::new(InvalidOid),
            gt_opr: Cell::new(InvalidOid),
            cmp_proc: Cell::new(InvalidOid),
            hash_proc: Cell::new(InvalidOid),
            hash_extended_proc: Cell::new(InvalidOid),
            eq_opr_finfo: RefCell::new(FmgrInfo::unresolved()),
            cmp_proc_finfo: RefCell::new(FmgrInfo::unresolved()),
            hash_proc_finfo: RefCell::new(FmgrInfo::unresolved()),
            hash_extended_proc_finfo: RefCell::new(FmgrInfo::unresolved()),
            rng_collation: Cell::new(InvalidOid),
            rng_opfamily: Cell::new(InvalidOid),
            rng_elemtype: RefCell::new(None),
            rng_cmp_proc_finfo: RefCell::new(FmgrInfo::unresolved()),
            rng_canonical_finfo: RefCell::new(FmgrInfo::unresolved()),
            rng_subdiff_finfo: RefCell::new(FmgrInfo::unresolved()),
            rngtype: RefCell::new(None),
            domain_base_type: Cell::new(InvalidOid),
            domain_base_typmod: Cell::new(-1),
            domain_data: Cell::new(None),
            flags: Cell::new(0),
            ready: Cell::new(0),
            next_domain: Cell::new(InvalidOid),
            enum_data: RefCell::new(None),
        }
    }

    cell_getters!(
        typlen: i16,
        typbyval: bool,
        typalign: i8,
        typstorage: i8,
        typtype: i8,
        typrelid: Oid,
        typsubscript: Oid,
        typelem: Oid,
        typarray: Oid,
        typcollation: Oid,
        btree_opf: Oid,
        btree_opintype: Oid,
        hash_opf: Oid,
        hash_opintype: Oid,
        eq_opr: Oid,
        lt_opr: Oid,
        gt_opr: Oid,
        cmp_proc: Oid,
        hash_proc: Oid,
        hash_extended_proc: Oid,
    );

    // Borrows are per-call: a finfo whose resolved function re-enters the same
    // entry's same finfo (C aliases the pointer) panics loudly instead.
    pub fn eq_opr_finfo(&self) -> RefMut<'_, FmgrInfo> {
        self.eq_opr_finfo.borrow_mut()
    }
    pub fn cmp_proc_finfo(&self) -> RefMut<'_, FmgrInfo> {
        self.cmp_proc_finfo.borrow_mut()
    }
    pub fn hash_proc_finfo(&self) -> RefMut<'_, FmgrInfo> {
        self.hash_proc_finfo.borrow_mut()
    }
    pub fn hash_extended_proc_finfo(&self) -> RefMut<'_, FmgrInfo> {
        self.hash_extended_proc_finfo.borrow_mut()
    }
    pub fn rng_cmp_proc_finfo(&self) -> RefMut<'_, FmgrInfo> {
        self.rng_cmp_proc_finfo.borrow_mut()
    }
    pub fn rng_canonical_finfo(&self) -> RefMut<'_, FmgrInfo> {
        self.rng_canonical_finfo.borrow_mut()
    }
    pub fn rng_subdiff_finfo(&self) -> RefMut<'_, FmgrInfo> {
        self.rng_subdiff_finfo.borrow_mut()
    }
    pub fn rng_collation(&self) -> Oid {
        self.rng_collation.get()
    }
    pub fn rng_opfamily(&self) -> Oid {
        self.rng_opfamily.get()
    }
    /// C's `rngelemtype`; `None` mirrors the NULL pointer ("not a range type").
    pub fn rngelemtype(&self) -> Option<Rc<TypeCacheEntry>> {
        self.rng_elemtype.borrow().clone()
    }
    /// C's `rngtype`; `None` mirrors the NULL pointer ("not a multirange type").
    pub fn rngtype(&self) -> Option<Rc<TypeCacheEntry>> {
        self.rngtype.borrow().clone()
    }

    #[inline]
    pub(crate) fn set_flags(&self, bits: i32) {
        self.flags.set(self.flags.get() | bits);
    }
    #[inline]
    pub(crate) fn clear_flags(&self, bits: i32) {
        self.flags.set(self.flags.get() & !bits);
    }
    #[inline]
    pub(crate) fn flags_raw(&self) -> i32 {
        self.flags.get()
    }
    #[inline]
    pub(crate) fn set_ready(&self, r: i32) {
        self.ready.set(r);
    }
    #[inline]
    pub fn domain_base_type(&self) -> Oid {
        self.domain_base_type.get()
    }
    #[inline]
    pub fn domain_base_typmod(&self) -> i32 {
        self.domain_base_typmod.get()
    }
    #[inline]
    pub(crate) fn next_domain_get(&self) -> Oid {
        self.next_domain.get()
    }

    /// Proofs-only seam constructor. Builds a concrete, cache-independent
    /// entry populated with exactly the fields the record/array compare
    /// paths read (type_id, typlen/typbyval/typalign, eq_opr/cmp_proc and
    /// their finfos), so a proofs crate can instantiate e.g. an int4 entry
    /// without the thread-local cache or catalog. Never called in shipping
    /// code; all other fields stay at their pre-fill defaults.
    #[cfg(any(test, feature = "proof-support"))]
    #[allow(clippy::too_many_arguments)]
    pub fn proof_entry_for_compare(
        type_id: Oid,
        typlen: i16,
        typbyval: bool,
        typalign: i8,
        eq_opr: Oid,
        eq_opr_finfo: FmgrInfo,
        cmp_proc: Oid,
        cmp_proc_finfo: FmgrInfo,
    ) -> Rc<TypeCacheEntry> {
        let e = TypeCacheEntry::new(type_id, 0);
        e.typlen.set(typlen);
        e.typbyval.set(typbyval);
        e.typalign.set(typalign);
        e.eq_opr.set(eq_opr);
        e.cmp_proc.set(cmp_proc);
        *e.eq_opr_finfo.borrow_mut() = eq_opr_finfo;
        *e.cmp_proc_finfo.borrow_mut() = cmp_proc_finfo;
        Rc::new(e)
    }
}

pub(crate) struct TypCacheState {
    #[allow(dead_code)]
    pub(crate) mcx: Mcx<'static>,
    pub(crate) type_cache: PgHashMap<'static, Oid, Rc<TypeCacheEntry>>,
    pub(crate) rel_id_to_type_id: PgHashMap<'static, Oid, Oid>,
    pub(crate) first_domain_type_entry: Oid,
    pub(crate) in_progress: PgVec<'static, Oid>,
    pub(crate) callbacks_registered: bool,
    // C's RecordCacheArray/SharedRecordTypmodRegistry: anonymous rowtypes
    // registered by typmod index. A parallel worker installs the leader's
    // handle here in place of its own (typcache_seams::install_record_registry).
    pub(crate) record_registry: typcache_seams::RecordRegistryHandle,
    pub(crate) tupledesc_id_counter: u64,
}

thread_local! {
    static STATE: RefCell<Option<ManuallyDrop<TypCacheState>>> = const { RefCell::new(None) };
}

// INVARIANT: `f` must not call back into any seam or re-entrant typcache path;
// the borrow is held for its whole extent (loud RefCell panic otherwise).
pub(crate) fn with_state<R>(f: impl FnOnce(&mut TypCacheState) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            let mcx = ::mcx::session_root("TypCacheContext").mcx();
            // LIFO: drop the state PROPERLY before the context free — cache
            // entries are Rc's on the GLOBAL heap (the relcache-entry class);
            // a context-only free skips the refcount decrements.
            ::mcx::register_session_cleanup(Box::new(|| {
                STATE.with(|cell| {
                    if let Some(st) = cell.borrow_mut().take() {
                        drop(ManuallyDrop::into_inner(st));
                    }
                });
            }));
            ManuallyDrop::new(TypCacheState {
                mcx,
                type_cache: PgHashMap::with_capacity_in(64, mcx),
                rel_id_to_type_id: PgHashMap::with_capacity_in(64, mcx),
                first_domain_type_entry: InvalidOid,
                in_progress: PgVec::new_in(mcx),
                callbacks_registered: false,
                record_registry: Default::default(),
                tupledesc_id_counter: 0,
            })
        });
        f(st)
    })
}

/// lookup_type_cache (typcache.c). The pin is C's returned `TypeCacheEntry *`.
///
/// Warm-hit divergence: C re-walks every per-flag "checked?" branch (and
/// pushes/pops in_progress_list) on every call; here `ready` caches the union
/// of satisfiable request bits, so a warm hit is one hash probe + one mask
/// test. `ready` shrinks in the inval callbacks in lockstep with TCFLAGS, and
/// the slow path is a faithful transcription of C's fill.
#[inline]
pub fn lookup_type_cache(type_id: Oid, flags: i32) -> PgResult<Rc<TypeCacheEntry>> {
    let hit = with_state(|st| {
        st.type_cache.get(&type_id).and_then(|e| {
            (((flags | READY_PG_TYPE_DATA) & !e.ready.get()) == 0).then(|| Rc::clone(e))
        })
    });
    match hit {
        Some(e) => Ok(e),
        None => lookup_type_cache_slow(type_id, flags),
    }
}

#[track_caller]
#[cold]
fn type_does_not_exist(type_id: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("type with OID {type_id} does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

#[track_caller]
#[cold]
fn shell_type(t: &PgTypeTypcacheShape) -> Box<PgError> {
    let name = String::from_utf8_lossy(t.typname.name_str()).into_owned();
    Box::new(
        PgError::error(format!("type \"{name}\" is only a shell"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

#[cold]
#[inline(never)]
pub(crate) fn lane_unported(lane: &str, type_id: Oid) -> ! {
    panic!("typcache: {lane} lane not ported (type {type_id})");
}

fn load_pg_type_row(type_id: Oid) -> PgResult<PgTypeTypcacheShape> {
    let Some(t) = syscache_seams::lookup_pg_type_typcache_shape::call(type_id)? else {
        return Err(type_does_not_exist(type_id));
    };
    if !t.typisdefined {
        return Err(shell_type(&t));
    }
    Ok(t)
}

fn copy_pg_type_fields(e: &TypeCacheEntry, t: &PgTypeTypcacheShape) {
    e.typlen.set(t.typlen);
    e.typbyval.set(t.typbyval);
    e.typalign.set(t.typalign);
    e.typstorage.set(t.typstorage);
    e.typtype.set(t.typtype);
    e.typrelid.set(t.typrelid);
    e.typsubscript.set(t.typsubscript);
    e.typelem.set(t.typelem);
    e.typarray.set(t.typarray);
    e.typcollation.set(t.typcollation);
    e.set_flags(TCFLAGS_HAVE_PG_TYPE_DATA);
}

#[inline(never)]
fn lookup_type_cache_slow(type_id: Oid, mut flags: i32) -> PgResult<Rc<TypeCacheEntry>> {
    if !with_state(|st| st.callbacks_registered) {
        inval::invalidate::CacheRegisterRelcacheCallback(
            invalidate::TypeCacheRelCallback,
            datum::Datum::from_oid(InvalidOid),
        )?;
        inval::invalidate::CacheRegisterSyscacheCallback(
            TYPEOID,
            invalidate::TypeCacheTypCallback,
            datum::Datum::from_oid(InvalidOid),
        )?;
        inval::invalidate::CacheRegisterSyscacheCallback(
            CLAOID,
            invalidate::TypeCacheOpcCallback,
            datum::Datum::from_oid(InvalidOid),
        )?;
        inval::invalidate::CacheRegisterSyscacheCallback(
            CONSTROID,
            invalidate::TypeCacheConstrCallback,
            datum::Datum::from_oid(InvalidOid),
        )?;
        with_state(|st| {
            st.callbacks_registered = true;
            st.in_progress.reserve(4);
        });
    }

    // On an ereport path the pushed slot stays for
    // finalize_in_progress_typentries (AtEOXact/AtEOSubXact), as in C.
    let in_progress_offset = with_state(|st| {
        st.in_progress.push(type_id);
        st.in_progress.len() - 1
    });

    let entry = match with_state(|st| st.type_cache.get(&type_id).map(Rc::clone)) {
        Some(e) => {
            if e.flags.get() & TCFLAGS_HAVE_PG_TYPE_DATA == 0 {
                let t = load_pg_type_row(type_id)?;
                copy_pg_type_fields(&e, &t);
            }
            e
        }
        None => {
            let t = load_pg_type_row(type_id)?;
            let type_id_hash = syscache_seams::syscache_hash_value_typeoid::call(type_id)?;
            let e = Rc::new(TypeCacheEntry::new(type_id, type_id_hash));
            copy_pg_type_fields(&e, &t);
            with_state(|st| {
                if e.typtype.get() == TYPTYPE_DOMAIN {
                    e.next_domain.set(st.first_domain_type_entry);
                    st.first_domain_type_entry = type_id;
                }
                let prev = st.type_cache.insert(type_id, Rc::clone(&e));
                debug_assert!(prev.is_none(), "it wasn't there a moment ago");
            });
            e
        }
    };

    fill_entry(&entry, &mut flags)?;
    entry.ready.set(compute_ready(&entry));

    with_state(|st| {
        debug_assert_eq!(in_progress_offset + 1, st.in_progress.len());
        st.in_progress.pop();
    });

    with_state(|st| {
        let TypCacheState { rel_id_to_type_id, .. } = st;
        invalidate::insert_rel_type_cache_if_needed(rel_id_to_type_id, &entry);
    });

    Ok(entry)
}

fn fill_entry(e: &TypeCacheEntry, flags: &mut i32) -> PgResult<()> {
    let type_id = e.type_id;

    if (*flags
        & (TYPECACHE_EQ_OPR
            | TYPECACHE_LT_OPR
            | TYPECACHE_GT_OPR
            | TYPECACHE_CMP_PROC
            | TYPECACHE_EQ_OPR_FINFO
            | TYPECACHE_CMP_PROC_FINFO
            | TYPECACHE_BTREE_OPFAMILY))
        != 0
        && e.flags.get() & TCFLAGS_CHECKED_BTREE_OPCLASS == 0
    {
        let opclass = indexcmds_seams::get_default_opclass::call(type_id, BTREE_AM_OID)?;
        if opclass != InvalidOid {
            e.btree_opf.set(get_opclass_family(opclass)?);
            e.btree_opintype.set(get_opclass_input_type(opclass)?);
        } else {
            e.btree_opf.set(InvalidOid);
            e.btree_opintype.set(InvalidOid);
        }
        e.clear_flags(
            TCFLAGS_CHECKED_EQ_OPR
                | TCFLAGS_CHECKED_LT_OPR
                | TCFLAGS_CHECKED_GT_OPR
                | TCFLAGS_CHECKED_CMP_PROC,
        );
        e.set_flags(TCFLAGS_CHECKED_BTREE_OPCLASS);
    }

    if (*flags & (TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO)) != 0
        && e.flags.get() & TCFLAGS_CHECKED_EQ_OPR == 0
        && e.btree_opf.get() == InvalidOid
    {
        *flags |= TYPECACHE_HASH_OPFAMILY;
    }

    if (*flags
        & (TYPECACHE_HASH_PROC
            | TYPECACHE_HASH_PROC_FINFO
            | TYPECACHE_HASH_EXTENDED_PROC
            | TYPECACHE_HASH_EXTENDED_PROC_FINFO
            | TYPECACHE_HASH_OPFAMILY))
        != 0
        && e.flags.get() & TCFLAGS_CHECKED_HASH_OPCLASS == 0
    {
        let opclass = indexcmds_seams::get_default_opclass::call(type_id, HASH_AM_OID)?;
        if opclass != InvalidOid {
            e.hash_opf.set(get_opclass_family(opclass)?);
            e.hash_opintype.set(get_opclass_input_type(opclass)?);
        } else {
            e.hash_opf.set(InvalidOid);
            e.hash_opintype.set(InvalidOid);
        }
        e.clear_flags(TCFLAGS_CHECKED_HASH_PROC | TCFLAGS_CHECKED_HASH_EXTENDED_PROC);
        e.set_flags(TCFLAGS_CHECKED_HASH_OPCLASS);
    }

    if (*flags & (TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO)) != 0
        && e.flags.get() & TCFLAGS_CHECKED_EQ_OPR == 0
    {
        let mut eq_opr = InvalidOid;
        if e.btree_opf.get() != InvalidOid {
            eq_opr = get_opfamily_member(
                e.btree_opf.get(),
                e.btree_opintype.get(),
                e.btree_opintype.get(),
                BTEqualStrategyNumber,
            )?;
        }
        if eq_opr == InvalidOid && e.hash_opf.get() != InvalidOid {
            eq_opr = get_opfamily_member(
                e.hash_opf.get(),
                e.hash_opintype.get(),
                e.hash_opintype.get(),
                HTEqualStrategyNumber,
            )?;
        }
        if eq_opr == ARRAY_EQ_OP && !array_element_has(e, TCFLAGS_HAVE_ELEM_EQUALITY)? {
            eq_opr = InvalidOid;
        } else if eq_opr == RECORD_EQ_OP
            && !record_fields_have(e, TCFLAGS_HAVE_FIELD_EQUALITY)?
        {
            eq_opr = InvalidOid;
        }
        if e.eq_opr.get() != eq_opr {
            e.eq_opr_finfo.borrow_mut().fn_oid = InvalidOid;
        }
        e.eq_opr.set(eq_opr);
        e.clear_flags(TCFLAGS_CHECKED_HASH_PROC | TCFLAGS_CHECKED_HASH_EXTENDED_PROC);
        e.set_flags(TCFLAGS_CHECKED_EQ_OPR);
    }

    if (*flags & TYPECACHE_LT_OPR) != 0 && e.flags.get() & TCFLAGS_CHECKED_LT_OPR == 0 {
        let mut lt_opr = InvalidOid;
        if e.btree_opf.get() != InvalidOid {
            lt_opr = get_opfamily_member(
                e.btree_opf.get(),
                e.btree_opintype.get(),
                e.btree_opintype.get(),
                BTLessStrategyNumber,
            )?;
        }
        if lt_opr == ARRAY_LT_OP && !array_element_has(e, TCFLAGS_HAVE_ELEM_COMPARE)? {
            lt_opr = InvalidOid;
        } else if lt_opr == RECORD_LT_OP
            && !record_fields_have(e, TCFLAGS_HAVE_FIELD_COMPARE)?
        {
            lt_opr = InvalidOid;
        }
        e.lt_opr.set(lt_opr);
        e.set_flags(TCFLAGS_CHECKED_LT_OPR);
    }

    if (*flags & TYPECACHE_GT_OPR) != 0 && e.flags.get() & TCFLAGS_CHECKED_GT_OPR == 0 {
        let mut gt_opr = InvalidOid;
        if e.btree_opf.get() != InvalidOid {
            gt_opr = get_opfamily_member(
                e.btree_opf.get(),
                e.btree_opintype.get(),
                e.btree_opintype.get(),
                BTGreaterStrategyNumber,
            )?;
        }
        if gt_opr == ARRAY_GT_OP && !array_element_has(e, TCFLAGS_HAVE_ELEM_COMPARE)? {
            gt_opr = InvalidOid;
        } else if gt_opr == RECORD_GT_OP
            && !record_fields_have(e, TCFLAGS_HAVE_FIELD_COMPARE)?
        {
            gt_opr = InvalidOid;
        }
        e.gt_opr.set(gt_opr);
        e.set_flags(TCFLAGS_CHECKED_GT_OPR);
    }

    if (*flags & (TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO)) != 0
        && e.flags.get() & TCFLAGS_CHECKED_CMP_PROC == 0
    {
        let mut cmp_proc = InvalidOid;
        if e.btree_opf.get() != InvalidOid {
            cmp_proc = get_opfamily_proc(
                e.btree_opf.get(),
                e.btree_opintype.get(),
                e.btree_opintype.get(),
                BTORDER_PROC,
            )?;
        }
        if cmp_proc == F_BTARRAYCMP && !array_element_has(e, TCFLAGS_HAVE_ELEM_COMPARE)? {
            cmp_proc = InvalidOid;
        } else if cmp_proc == F_BTRECORDCMP
            && !record_fields_have(e, TCFLAGS_HAVE_FIELD_COMPARE)?
        {
            cmp_proc = InvalidOid;
        }
        if e.cmp_proc.get() != cmp_proc {
            e.cmp_proc_finfo.borrow_mut().fn_oid = InvalidOid;
        }
        e.cmp_proc.set(cmp_proc);
        e.set_flags(TCFLAGS_CHECKED_CMP_PROC);
    }

    if (*flags & (TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO)) != 0
        && e.flags.get() & TCFLAGS_CHECKED_HASH_PROC == 0
    {
        let mut hash_proc = resolve_hash_proc(e, HASHSTANDARD_PROC)?;
        if hash_proc == F_HASH_ARRAY && !array_element_has(e, TCFLAGS_HAVE_ELEM_HASHING)? {
            hash_proc = InvalidOid;
        } else if hash_proc == F_HASH_RECORD
            && !record_fields_have(e, TCFLAGS_HAVE_FIELD_HASHING)?
        {
            hash_proc = InvalidOid;
        } else if hash_proc == F_HASH_RANGE && !range_element_has(e, TCFLAGS_HAVE_ELEM_HASHING)? {
            hash_proc = InvalidOid;
        }
        if hash_proc == F_HASH_MULTIRANGE
            && !multirange_element_has(e, TCFLAGS_HAVE_ELEM_HASHING)?
        {
            hash_proc = InvalidOid;
        }
        if e.hash_proc.get() != hash_proc {
            e.hash_proc_finfo.borrow_mut().fn_oid = InvalidOid;
        }
        e.hash_proc.set(hash_proc);
        e.set_flags(TCFLAGS_CHECKED_HASH_PROC);
    }

    if (*flags & (TYPECACHE_HASH_EXTENDED_PROC | TYPECACHE_HASH_EXTENDED_PROC_FINFO)) != 0
        && e.flags.get() & TCFLAGS_CHECKED_HASH_EXTENDED_PROC == 0
    {
        let mut hash_extended_proc = resolve_hash_proc(e, HASHEXTENDED_PROC)?;
        if hash_extended_proc == F_HASH_ARRAY_EXTENDED
            && !array_element_has(e, TCFLAGS_HAVE_ELEM_EXTENDED_HASHING)?
        {
            hash_extended_proc = InvalidOid;
        } else if hash_extended_proc == F_HASH_RECORD_EXTENDED
            && !record_fields_have(e, TCFLAGS_HAVE_FIELD_EXTENDED_HASHING)?
        {
            hash_extended_proc = InvalidOid;
        } else if hash_extended_proc == F_HASH_RANGE_EXTENDED
            && !range_element_has(e, TCFLAGS_HAVE_ELEM_EXTENDED_HASHING)?
        {
            hash_extended_proc = InvalidOid;
        }
        if hash_extended_proc == F_HASH_MULTIRANGE_EXTENDED
            && !multirange_element_has(e, TCFLAGS_HAVE_ELEM_EXTENDED_HASHING)?
        {
            hash_extended_proc = InvalidOid;
        }
        if e.hash_extended_proc.get() != hash_extended_proc {
            e.hash_extended_proc_finfo.borrow_mut().fn_oid = InvalidOid;
        }
        e.hash_extended_proc.set(hash_extended_proc);
        e.set_flags(TCFLAGS_CHECKED_HASH_EXTENDED_PROC);
    }

    if (*flags & TYPECACHE_EQ_OPR_FINFO) != 0
        && e.eq_opr_finfo.borrow().fn_oid == InvalidOid
        && e.eq_opr.get() != InvalidOid
    {
        let eq_opr_func = get_opcode(e.eq_opr.get())?;
        if eq_opr_func != InvalidOid {
            *e.eq_opr_finfo.borrow_mut() = fmgr_seams::fmgr_info::call(eq_opr_func)?;
        }
    }
    if (*flags & TYPECACHE_CMP_PROC_FINFO) != 0
        && e.cmp_proc_finfo.borrow().fn_oid == InvalidOid
        && e.cmp_proc.get() != InvalidOid
    {
        *e.cmp_proc_finfo.borrow_mut() = fmgr_seams::fmgr_info::call(e.cmp_proc.get())?;
    }
    if (*flags & TYPECACHE_HASH_PROC_FINFO) != 0
        && e.hash_proc_finfo.borrow().fn_oid == InvalidOid
        && e.hash_proc.get() != InvalidOid
    {
        *e.hash_proc_finfo.borrow_mut() = fmgr_seams::fmgr_info::call(e.hash_proc.get())?;
    }
    if (*flags & TYPECACHE_HASH_EXTENDED_PROC_FINFO) != 0
        && e.hash_extended_proc_finfo.borrow().fn_oid == InvalidOid
        && e.hash_extended_proc.get() != InvalidOid
    {
        *e.hash_extended_proc_finfo.borrow_mut() =
            fmgr_seams::fmgr_info::call(e.hash_extended_proc.get())?;
    }

    if (*flags & TYPECACHE_TUPDESC) != 0 && e.typtype.get() == TYPTYPE_COMPOSITE {
        lane_unported("composite TUPDESC", type_id);
    }
    if (*flags & TYPECACHE_RANGE_INFO) != 0 && e.typtype.get() == TYPTYPE_RANGE {
        match e.rngelemtype() {
            None => load_rangetype_info(e)?,
            Some(el) => {
                if el.flags.get() & TCFLAGS_HAVE_PG_TYPE_DATA == 0 {
                    let _ = lookup_type_cache(el.type_id, 0)?;
                }
            }
        }
    }
    if (*flags & TYPECACHE_MULTIRANGE_INFO) != 0
        && e.rngtype.borrow().is_none()
        && e.typtype.get() == TYPTYPE_MULTIRANGE
    {
        load_multirangetype_info(e)?;
    }
    if (*flags & TYPECACHE_DOMAIN_BASE_INFO) != 0
        && e.domain_base_type.get() == InvalidOid
        && e.typtype.get() == TYPTYPE_DOMAIN
    {
        let mut typmod = -1;
        e.domain_base_type.set(lsyscache::getBaseTypeAndTypmod(type_id, &mut typmod)?);
        e.domain_base_typmod.set(typmod);
        if lsyscache::get_typ_typrelid(e.domain_base_type.get())? != InvalidOid {
            e.set_flags(TCFLAGS_DOMAIN_BASE_IS_COMPOSITE);
        }
    }
    if (*flags & TYPECACHE_DOMAIN_CONSTR_INFO) != 0
        && e.flags.get() & TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS == 0
        && e.typtype.get() == TYPTYPE_DOMAIN
    {
        domain::load_domaintype_info(e)?;
    }

    Ok(())
}

fn resolve_hash_proc(e: &TypeCacheEntry, procnum: i16) -> PgResult<Oid> {
    if e.hash_opf.get() == InvalidOid {
        return Ok(InvalidOid);
    }
    // C: the eq_opr, if determined, must match the hash opclass.
    let eq_ok = e.eq_opr.get() == InvalidOid
        || e.eq_opr.get()
            == get_opfamily_member(
                e.hash_opf.get(),
                e.hash_opintype.get(),
                e.hash_opintype.get(),
                HTEqualStrategyNumber,
            )?;
    if eq_ok {
        get_opfamily_proc(e.hash_opf.get(), e.hash_opintype.get(), e.hash_opintype.get(), procnum)
    } else {
        Ok(InvalidOid)
    }
}

#[track_caller]
#[cold]
fn missing_btorder_proc(opcintype: Oid, opfamily: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "missing support function {BTORDER_PROC}({opcintype},{opcintype}) in opfamily {opfamily}"
    )))
}

fn load_rangetype_info(e: &TypeCacheEntry) -> PgResult<()> {
    let Some(r) = syscache_seams::lookup_pg_range_shape::call(e.type_id)? else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for range type {}",
            e.type_id
        ))));
    };
    e.rng_collation.set(r.rngcollation);
    let opfamily = get_opclass_family(r.rngsubopc)?;
    let opcintype = get_opclass_input_type(r.rngsubopc)?;
    e.rng_opfamily.set(opfamily);
    let cmp_fn = get_opfamily_proc(opfamily, opcintype, opcintype, BTORDER_PROC)?;
    if cmp_fn == InvalidOid {
        return Err(missing_btorder_proc(opcintype, opfamily));
    }
    *e.rng_cmp_proc_finfo.borrow_mut() = fmgr_seams::fmgr_info::call(cmp_fn)?;
    if r.rngcanonical != InvalidOid {
        *e.rng_canonical_finfo.borrow_mut() = fmgr_seams::fmgr_info::call(r.rngcanonical)?;
    }
    if r.rngsubdiff != InvalidOid {
        *e.rng_subdiff_finfo.borrow_mut() = fmgr_seams::fmgr_info::call(r.rngsubdiff)?;
    }
    // C: assigning rngelemtype last marks the range data valid.
    let elem = lookup_type_cache(r.rngsubtype, 0)?;
    *e.rng_elemtype.borrow_mut() = Some(elem);
    Ok(())
}

fn load_multirangetype_info(e: &TypeCacheEntry) -> PgResult<()> {
    let range_oid = lsyscache::get_multirange_range(e.type_id)?;
    if range_oid == InvalidOid {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for multirange type {}",
            e.type_id
        ))));
    }
    let rngtype = lookup_type_cache(range_oid, TYPECACHE_RANGE_INFO)?;
    *e.rngtype.borrow_mut() = Some(rngtype);
    Ok(())
}

fn range_element_has(e: &TypeCacheEntry, have_bit: i32) -> PgResult<bool> {
    if e.flags.get() & TCFLAGS_CHECKED_ELEM_PROPERTIES == 0 {
        if e.rng_elemtype.borrow().is_none() && e.typtype.get() == TYPTYPE_RANGE {
            load_rangetype_info(e)?;
        }
        let elem = e.rngelemtype();
        if let Some(el) = elem {
            propagate_elem_hash_flags(e, el.type_id)?;
        }
        e.set_flags(TCFLAGS_CHECKED_ELEM_PROPERTIES);
    }
    Ok(e.flags.get() & have_bit != 0)
}

fn multirange_element_has(e: &TypeCacheEntry, have_bit: i32) -> PgResult<bool> {
    if e.flags.get() & TCFLAGS_CHECKED_ELEM_PROPERTIES == 0 {
        if e.rngtype.borrow().is_none() && e.typtype.get() == TYPTYPE_MULTIRANGE {
            load_multirangetype_info(e)?;
        }
        let elem = e.rngtype().and_then(|rt| rt.rngelemtype());
        if let Some(el) = elem {
            propagate_elem_hash_flags(e, el.type_id)?;
        }
        e.set_flags(TCFLAGS_CHECKED_ELEM_PROPERTIES);
    }
    Ok(e.flags.get() & have_bit != 0)
}

fn propagate_elem_hash_flags(e: &TypeCacheEntry, elem_oid: Oid) -> PgResult<()> {
    let el = lookup_type_cache(elem_oid, TYPECACHE_HASH_PROC | TYPECACHE_HASH_EXTENDED_PROC)?;
    if el.hash_proc.get() != InvalidOid {
        e.set_flags(TCFLAGS_HAVE_ELEM_HASHING);
    }
    if el.hash_extended_proc.get() != InvalidOid {
        e.set_flags(TCFLAGS_HAVE_ELEM_EXTENDED_HASHING);
    }
    Ok(())
}

fn record_fields_have(e: &TypeCacheEntry, have_bit: i32) -> PgResult<bool> {
    if e.flags.get() & TCFLAGS_CHECKED_FIELD_PROPERTIES == 0 {
        cache_record_field_properties(e)?;
    }
    Ok(e.flags.get() & have_bit != 0)
}

// C cache_record_field_properties: RECORD is assumed sortable (a wrong guess
// costs a runtime error, matching C); composites check every non-dropped
// field; domain-over-composite is loud.
fn cache_record_field_properties(e: &TypeCacheEntry) -> PgResult<()> {
    if e.type_id == types_core::catalog::RECORDOID {
        e.set_flags(TCFLAGS_HAVE_FIELD_EQUALITY | TCFLAGS_HAVE_FIELD_COMPARE);
    } else if e.typtype.get() == TYPTYPE_COMPOSITE {
        let mcx = with_state(|st| st.mcx);
        let tupdesc = lookup_rowtype_tupdesc_copy(mcx, e.type_id, -1)?;
        let mut newflags = TCFLAGS_HAVE_FIELD_EQUALITY
            | TCFLAGS_HAVE_FIELD_COMPARE
            | TCFLAGS_HAVE_FIELD_HASHING
            | TCFLAGS_HAVE_FIELD_EXTENDED_HASHING;
        for i in 0..tupdesc.natts as usize {
            let attr = tupdesc.attr(i);
            if attr.attisdropped {
                continue;
            }
            let fe = lookup_type_cache(
                attr.atttypid,
                TYPECACHE_EQ_OPR | TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC
                    | TYPECACHE_HASH_EXTENDED_PROC,
            )?;
            if fe.eq_opr.get() == InvalidOid {
                newflags &= !TCFLAGS_HAVE_FIELD_EQUALITY;
            }
            if fe.cmp_proc.get() == InvalidOid {
                newflags &= !TCFLAGS_HAVE_FIELD_COMPARE;
            }
            if fe.hash_proc.get() == InvalidOid {
                newflags &= !TCFLAGS_HAVE_FIELD_HASHING;
            }
            if fe.hash_extended_proc.get() == InvalidOid {
                newflags &= !TCFLAGS_HAVE_FIELD_EXTENDED_HASHING;
            }
            if newflags == 0 {
                break;
            }
        }
        e.set_flags(newflags);
    } else if e.typtype.get() == TYPTYPE_DOMAIN {
        lane_unported("record field-properties over a domain", e.type_id);
    }
    e.set_flags(TCFLAGS_CHECKED_FIELD_PROPERTIES);
    Ok(())
}

fn array_element_has(e: &TypeCacheEntry, have_bit: i32) -> PgResult<bool> {
    if e.flags.get() & TCFLAGS_CHECKED_ELEM_PROPERTIES == 0 {
        cache_array_element_properties(e)?;
    }
    Ok(e.flags.get() & have_bit != 0)
}

fn cache_array_element_properties(e: &TypeCacheEntry) -> PgResult<()> {
    let elem_type = get_base_element_type(e.type_id)?;
    if elem_type != InvalidOid {
        let el = lookup_type_cache(
            elem_type,
            TYPECACHE_EQ_OPR | TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC
                | TYPECACHE_HASH_EXTENDED_PROC,
        )?;
        if el.eq_opr.get() != InvalidOid {
            e.set_flags(TCFLAGS_HAVE_ELEM_EQUALITY);
        }
        if el.cmp_proc.get() != InvalidOid {
            e.set_flags(TCFLAGS_HAVE_ELEM_COMPARE);
        }
        if el.hash_proc.get() != InvalidOid {
            e.set_flags(TCFLAGS_HAVE_ELEM_HASHING);
        }
        if el.hash_extended_proc.get() != InvalidOid {
            e.set_flags(TCFLAGS_HAVE_ELEM_EXTENDED_HASHING);
        }
    }
    e.set_flags(TCFLAGS_CHECKED_ELEM_PROPERTIES);
    Ok(())
}

// A finfo whose RefCell is currently borrowed is necessarily resolved (holders
// obtained it from a *_FINFO lookup); reentrant lookups from inside such a call
// (range_cmp/record_cmp fn_extra fills) must not panic here.
fn finfo_resolved(c: &RefCell<FmgrInfo>) -> bool {
    c.try_borrow().map_or(true, |f| f.fn_oid != InvalidOid)
}

pub(crate) fn compute_ready(e: &TypeCacheEntry) -> i32 {
    let f = e.flags.get();
    if f & TCFLAGS_HAVE_PG_TYPE_DATA == 0 {
        return 0;
    }
    let mut r = READY_PG_TYPE_DATA;
    if f & TCFLAGS_CHECKED_BTREE_OPCLASS != 0 {
        r |= TYPECACHE_BTREE_OPFAMILY;
    }
    if f & TCFLAGS_CHECKED_HASH_OPCLASS != 0 {
        r |= TYPECACHE_HASH_OPFAMILY;
    }
    if f & TCFLAGS_CHECKED_EQ_OPR != 0 {
        r |= TYPECACHE_EQ_OPR;
        if e.eq_opr.get() == InvalidOid || finfo_resolved(&e.eq_opr_finfo) {
            r |= TYPECACHE_EQ_OPR_FINFO;
        }
    }
    if f & TCFLAGS_CHECKED_LT_OPR != 0 {
        r |= TYPECACHE_LT_OPR;
    }
    if f & TCFLAGS_CHECKED_GT_OPR != 0 {
        r |= TYPECACHE_GT_OPR;
    }
    if f & TCFLAGS_CHECKED_CMP_PROC != 0 {
        r |= TYPECACHE_CMP_PROC;
        if e.cmp_proc.get() == InvalidOid || finfo_resolved(&e.cmp_proc_finfo) {
            r |= TYPECACHE_CMP_PROC_FINFO;
        }
    }
    if f & TCFLAGS_CHECKED_HASH_PROC != 0 {
        r |= TYPECACHE_HASH_PROC;
        if e.hash_proc.get() == InvalidOid || finfo_resolved(&e.hash_proc_finfo) {
            r |= TYPECACHE_HASH_PROC_FINFO;
        }
    }
    if f & TCFLAGS_CHECKED_HASH_EXTENDED_PROC != 0 {
        r |= TYPECACHE_HASH_EXTENDED_PROC;
        if e.hash_extended_proc.get() == InvalidOid
            || finfo_resolved(&e.hash_extended_proc_finfo)
        {
            r |= TYPECACHE_HASH_EXTENDED_PROC_FINFO;
        }
    }
    // Unported lanes stay slow-path (loud) for the typtypes they apply to; for
    // every other typtype C's arm is a no-op, so the bit is warm-satisfiable.
    let tt = e.typtype.get();
    if tt != TYPTYPE_COMPOSITE {
        r |= TYPECACHE_TUPDESC;
    }
    // RANGE_INFO is never warm-satisfiable for a real range type: C re-checks
    // the element entry's pg_type data on every hit (typcache.c:918-925).
    if tt != TYPTYPE_RANGE {
        r |= TYPECACHE_RANGE_INFO;
    }
    if tt != TYPTYPE_MULTIRANGE || e.rngtype.try_borrow().map_or(true, |r| r.is_some()) {
        r |= TYPECACHE_MULTIRANGE_INFO;
    }
    if tt != TYPTYPE_DOMAIN {
        r |= TYPECACHE_DOMAIN_BASE_INFO | TYPECACHE_DOMAIN_CONSTR_INFO;
    } else {
        if e.domain_base_type.get() != InvalidOid {
            r |= TYPECACHE_DOMAIN_BASE_INFO;
        }
        if f & TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS != 0 {
            r |= TYPECACHE_DOMAIN_CONSTR_INFO;
        }
    }
    r
}

pub fn init_seams() {
    typcache_seams::domain_has_constraints::set(domain::DomainHasConstraints);
    enum_cmp::install_seam();
    typcache_seams::at_eoxact_type_cache::set(AtEOXact_TypeCache);
    typcache_seams::at_eosubxact_type_cache::set(AtEOSubXact_TypeCache);
    typcache_seams::assign_record_type_typmod::set(assign_record_type_typmod);
    typcache_seams::lookup_rowtype_tupdesc_copy::set(lookup_rowtype_tupdesc_copy);
    typcache_seams::record_registry_handle::set(|| {
        with_state(|st| std::sync::Arc::clone(&st.record_registry))
    });
    typcache_seams::install_record_registry::set(|handle| {
        with_state(|st| st.record_registry = handle)
    });
    typcache_seams::type_cache_cmp_proc::set(|type_id| {
        Ok(lookup_type_cache(type_id, TYPECACHE_CMP_PROC)?.cmp_proc())
    });
    typcache_seams::type_cache_hash_proc::set(|type_id| {
        Ok(lookup_type_cache(type_id, TYPECACHE_HASH_PROC)?.hash_proc())
    });
}

// C: equalRowTypes (attname/atttypid/atttypmod/natts).
fn row_types_equal(a: &types_tuple::TupleDescData<'_>, b: &[types_tuple::FormData_pg_attribute]) -> bool {
    if a.natts as usize != b.len() {
        return false;
    }
    for i in 0..a.natts as usize {
        let (x, y) = (a.attr(i), &b[i]);
        if x.attname.name_str() != y.attname.name_str()
            || x.atttypid != y.atttypid
            || x.atttypmod != y.atttypmod
        {
            return false;
        }
    }
    true
}

/// C: assign_record_type_typmod (typcache.c) over RecordCacheArray, played by
/// [`typcache_seams::SharedRecordRegistry`] (a thread-native
/// SharedRecordTypmodRegistry — see typcache_seams for why this piece, unlike
/// the rest of session.c's DSM, needs an explicit cross-thread handle).
pub fn assign_record_type_typmod(tupdesc: &mut types_tuple::TupleDescData<'_>) -> PgResult<()> {
    debug_assert_eq!(tupdesc.tdtypeid, types_core::catalog::RECORDOID);
    let handle = with_state(|st| std::sync::Arc::clone(&st.record_registry));
    let mut reg = handle.lock().unwrap_or_else(|e| e.into_inner());
    for (i, e) in reg.entries.iter().enumerate() {
        if row_types_equal(tupdesc, &e.attrs) {
            tupdesc.tdtypmod = i as i32;
            return Ok(());
        }
    }
    let attrs: Vec<_> = (0..tupdesc.natts as usize).map(|i| *tupdesc.attr(i)).collect();
    let id = with_state(|st| {
        st.tupledesc_id_counter += 1;
        st.tupledesc_id_counter
    });
    tupdesc.tdtypmod = reg.entries.len() as i32;
    reg.entries.push(typcache_seams::SharedRecordEntry { attrs, id });
    Ok(())
}

pub const INVALID_TUPLEDESC_IDENTIFIER: u64 = 0;

/// C: assign_record_type_identifier (typcache.c). C divergence: named
/// composites get a fresh identifier per call — this typcache keeps no
/// composite tupdesc cache to pin a stable identifier to. Equal ids still
/// imply the same tupdesc; consumers only lose cache hits, never correctness.
pub fn assign_record_type_identifier(type_id: Oid, typmod: i32) -> PgResult<u64> {
    if type_id != types_core::catalog::RECORDOID {
        return with_state(|st| {
            st.tupledesc_id_counter += 1;
            Ok(st.tupledesc_id_counter)
        });
    }
    let handle = with_state(|st| std::sync::Arc::clone(&st.record_registry));
    let reg = handle.lock().unwrap_or_else(|e| e.into_inner());
    match usize::try_from(typmod).ok().and_then(|i| reg.entries.get(i)) {
        Some(e) => Ok(e.id),
        None => Err(record_type_not_registered()),
    }
}

#[track_caller]
#[cold]
fn record_type_not_registered() -> Box<PgError> {
    Box::new(
        PgError::error("record type has not been registered")
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
    )
}

/// C: lookup_rowtype_tupdesc_copy (typcache.c) — registered records by
/// typmod; named composites via the relation's descriptor.
pub fn lookup_rowtype_tupdesc_copy<'mcx>(
    mcx: Mcx<'mcx>,
    type_id: Oid,
    typmod: i32,
) -> PgResult<types_tuple::TupleDescData<'mcx>> {
    if type_id == types_core::catalog::RECORDOID {
        let handle = with_state(|st| std::sync::Arc::clone(&st.record_registry));
        let reg = handle.lock().unwrap_or_else(|e| e.into_inner());
        let Some(e) = usize::try_from(typmod).ok().and_then(|i| reg.entries.get(i)) else {
            return Err(record_type_not_registered());
        };
        let mut d = tupdesc::CreateTupleDesc(mcx, &e.attrs)?;
        d.tdtypeid = types_core::catalog::RECORDOID;
        d.tdtypmod = typmod;
        return Ok(d);
    }
    let typrelid = lsyscache::get_typ_typrelid(type_id)?;
    if !types_core::OidIsValid(typrelid) {
        return Err(Box::new(
            PgError::error(format!("type {type_id} is not composite"))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    let rel = relation_seams::relation_open::call(mcx, typrelid, types_rel::lock::AccessShareLock)?;
    let mut d = tupdesc::CreateTupleDescCopy(mcx, rel.descr())?;
    d.tdtypeid = type_id;
    d.tdtypmod = -1;
    rel.close(types_rel::lock::AccessShareLock)?;
    Ok(d)
}
