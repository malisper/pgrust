//! D3.2 shared immutable relcache core (docs/design/connection-scaling.md §D3).
//!
//! The D3.0 census made the constraint explicit: `Rc<RelationData>` entries
//! carry THREAD-AFFINE state (rd_smgr thread-local VFD handles, refcount
//! Cells, in-transaction subid bookkeeping, pgstat links, amcache/supportinfo
//! rule-5 caches) and can never be shared wholesale. What CAN be shared is
//! the immutable catalog-derived material, split here into [`RelCoreShared`]:
//! the decoded pg_class row, the pg_attribute array (the dominant chunk of a
//! tupdesc), constraint sources, and the index-access arrays — all owned on
//! the global heap inside an `Arc` published in the l2cache map keyed by
//! (relid, db, relcache generation stripe).
//!
//! # The mirror trick (how a thread-local shell aliases shared bytes)
//!
//! The shell is a normal `RelationData<'static>` whose big arrays are
//! `PgVec`/`PgString` VIEWS over the core's buffers, constructed with
//! [`mirror_vec`]/[`mirror_string`]: `Vec::from_raw_parts_in` over the shared
//! bytes with a dedicated dummy allocator handle — a leaked Bump-backend
//! `MemoryContext` whose `deallocate` is a no-op by construction (bump.c has
//! no free; mcx's Bump deallocate arm is empty and touches no accounting
//! state, so cross-thread drops of mirrors are races on nothing). Mirrors are
//! only ever made of `Copy` elements (no drop glue revisits shared memory)
//! or of `PgString` structs living in thread memory whose byte buffers alias
//! the core. Mutable-per-thread pieces are copied, not aliased: the
//! `CompactAttribute` array (its `attcacheoff` Cells are written on the
//! deform path) and the `TupleConstr` node (small; its string payloads still
//! alias the core).
//!
//! # Lifetime guard
//!
//! `RelationData` has no spare field to hold the Arc (and 600+ literal sites
//! forbid adding one), so the guard rides the one refcounted handle every
//! shell carries: `rd_att`. A per-thread registry maps core → its mirror
//! `Rc<TupleDescData>` AND the `Arc<RelCoreShared>`; the registry entry is
//! pruned only when the registry itself is the last holder of the Rc. Every
//! mirror inside a shell (attrs, indkey, constraint strings, ...) is
//! therefore transitively kept alive by the shell's `rd_att` clone — clones
//! that outlive the shell (executors keep tupdescs) extend the core exactly
//! as long as needed. Mirror drop glue never dereferences the aliased
//! buffers, so teardown order is a non-issue.

use core::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use mcx::{Mcx, MemoryContext, PgString, PgVec};
use types_core::{
    AttrNumber, InvalidSubTransactionId, Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_TEMP,
};
use types_error::PgResult;
use types_rel::{FormData_pg_class, FormData_pg_index, RdOptions, RelationData};
use types_tuple::{
    AttrDefault, CompactAttribute, ConstrCheck, FormData_pg_attribute, TupleConstr, TupleDescData,
};

use crate::build::{RelationInitPhysicalAddr, RelationInitTableAccessMethod};
use crate::cache_mcx;

// ---------------------------------------------------------------------------
// Dummy allocator for mirrors
// ---------------------------------------------------------------------------

/// The mirrors' allocator handle: a leaked Bump-backend context that is never
/// allocated from and never dropped. Its ONLY cross-thread use is `deallocate`
/// when a mirror drops, and mcx's Bump deallocate arm is empty (`=> {}` —
/// no accounting writes, no arena access), so concurrent mirror drops from
/// many threads race on nothing.
fn mirror_mcx() -> Mcx<'static> {
    static CTX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    let addr = *CTX.get_or_init(|| {
        Box::leak(Box::new(MemoryContext::new_bumpforget("L2RelMirrorDummy")))
            as *const MemoryContext as usize
    });
    // SAFETY: leaked (never dropped), 'static by construction. MemoryContext
    // is !Sync, but the sole operations routed through this handle are no-op
    // Bump deallocates reading only the backend discriminant (see above).
    unsafe { (*(addr as *const MemoryContext)).mcx() }
}

/// A read-only `PgVec` view over core-owned bytes.
///
/// # Safety
/// `src` must be owned by an `Arc<RelCoreShared>` that provably outlives the
/// returned vec (the tupdesc-registry guard, module doc). `T` must be `Copy`
/// (no drop glue ever revisits the shared buffer). The view must never be
/// grown or mutated.
unsafe fn mirror_vec<T: Copy>(src: &[T]) -> PgVec<'static, T> {
    // SAFETY: caller contract; deallocate on drop is a no-op (mirror_mcx).
    unsafe {
        PgVec::from_raw_parts_in(src.as_ptr() as *mut T, src.len(), src.len(), mirror_mcx())
    }
}

/// A read-only `PgString` view over a core-owned string. Same contract as
/// [`mirror_vec`]; the `PgString` struct itself lives in thread memory.
unsafe fn mirror_string(src: &str) -> PgString<'static> {
    // SAFETY: caller contract (mirror_vec).
    let bytes = unsafe { mirror_vec::<u8>(src.as_bytes()) };
    PgString::from_utf8(bytes).expect("core strings are UTF-8")
}

// ---------------------------------------------------------------------------
// The shared core
// ---------------------------------------------------------------------------

pub(crate) struct ConstrCore {
    pub defval: Box<[(AttrNumber, Option<Box<str>>)]>,
    pub check: Box<[CheckCore]>,
    pub num_defval: u16,
    pub num_check: u16,
    pub has_not_null: bool,
    pub has_generated_stored: bool,
    pub has_generated_virtual: bool,
}

pub(crate) struct CheckCore {
    pub ccname: Option<Box<str>>,
    pub ccbin: Option<Box<str>>,
    pub ccenforced: bool,
    pub ccvalid: bool,
    pub ccnoinherit: bool,
}

pub(crate) struct IndexCore {
    pub indexrelid: Oid,
    pub indrelid: Oid,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisvalid: bool,
    pub indisready: bool,
    pub indkey: Box<[AttrNumber]>,
    pub has_indpred: bool,
    pub indexprs_src: Option<Box<str>>,
    pub indpred_src: Option<Box<str>>,
    pub opcintype: Box<[Oid]>,
    pub opfamily: Box<[Oid]>,
    pub indoption: Box<[i16]>,
    pub indcollation: Box<[Oid]>,
    pub support: Box<[Oid]>,
    /// fn_oid per preloaded rd_supportinfo slot (FmgrInfo itself is
    /// thread-affine: fn_extra memos); the shell re-resolves via fmgr_info.
    pub supportinfo_oids: Box<[Option<Oid>]>,
}

/// The shareable immutable core of one relcache entry.
pub(crate) struct RelCoreShared {
    pub relid: Oid,
    pub form: FormData_pg_class,
    pub relhastriggers: bool,
    pub relhasrules: bool,
    pub options: Option<RdOptions>,
    pub tdtypeid: Oid,
    pub tdtypmod: i32,
    pub attrs: Box<[FormData_pg_attribute]>,
    /// Template only: every shell takes a COPY (attcacheoff Cells are written
    /// per thread on the deform path; the template itself is never touched).
    pub compact: Box<[CompactAttribute]>,
    pub constr: Option<ConstrCore>,
    pub index: Option<IndexCore>,
}

// SAFETY: immutable after construction and publication. The lone interior-
// mutable member (CompactAttribute::attcacheoff Cells in `compact`) is a
// template that is only ever READ (cloned element-wise into per-thread
// copies), never written through, on any thread.
unsafe impl Send for RelCoreShared {}
// SAFETY: as above.
unsafe impl Sync for RelCoreShared {}

impl RelCoreShared {
    pub(crate) fn approx_bytes(&self) -> usize {
        let strings: usize = self
            .constr
            .as_ref()
            .map(|c| {
                c.defval
                    .iter()
                    .map(|(_, s)| s.as_deref().map_or(0, str::len))
                    .sum::<usize>()
                    + c.check
                        .iter()
                        .map(|k| {
                            k.ccname.as_deref().map_or(0, str::len)
                                + k.ccbin.as_deref().map_or(0, str::len)
                        })
                        .sum::<usize>()
            })
            .unwrap_or(0);
        let index: usize = self
            .index
            .as_ref()
            .map(|ix| {
                (ix.indkey.len() + ix.indoption.len()) * 2
                    + (ix.opcintype.len() + ix.opfamily.len() + ix.indcollation.len()
                        + ix.support.len())
                        * 4
                    + ix.indexprs_src.as_deref().map_or(0, str::len)
                    + ix.indpred_src.as_deref().map_or(0, str::len)
            })
            .unwrap_or(0);
        core::mem::size_of::<RelCoreShared>()
            + self.attrs.len() * core::mem::size_of::<FormData_pg_attribute>()
            + self.compact.len() * core::mem::size_of::<CompactAttribute>()
            + strings
            + index
    }

    /// Deep-copy a freshly built (valid, committed-catalog) entry into a
    /// shareable core. `None` = not shareable: temp/session-local relations
    /// (thread-affine by definition) and descriptors with `missing` attribute
    /// defaults (their Datums point into descriptor-owned bytes; deferred).
    pub(crate) fn from_built(rel: &RelationData<'static>) -> Option<RelCoreShared> {
        if rel.rd_islocaltemp
            || rel.rd_backend != INVALID_PROC_NUMBER
            || rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP
        {
            return None;
        }
        let td = &rel.rd_att;
        let constr = match &td.constr {
            None => None,
            Some(c) => {
                if !c.missing.is_empty() {
                    return None;
                }
                Some(ConstrCore {
                    defval: c
                        .defval
                        .iter()
                        .map(|d| (d.adnum, d.adbin.as_ref().map(|s| Box::from(s.as_str()))))
                        .collect(),
                    check: c
                        .check
                        .iter()
                        .map(|k| CheckCore {
                            ccname: k.ccname.as_ref().map(|s| Box::from(s.as_str())),
                            ccbin: k.ccbin.as_ref().map(|s| Box::from(s.as_str())),
                            ccenforced: k.ccenforced,
                            ccvalid: k.ccvalid,
                            ccnoinherit: k.ccnoinherit,
                        })
                        .collect(),
                    num_defval: c.num_defval,
                    num_check: c.num_check,
                    has_not_null: c.has_not_null,
                    has_generated_stored: c.has_generated_stored,
                    has_generated_virtual: c.has_generated_virtual,
                })
            }
        };
        let index = rel.rd_index.as_ref().map(|ix| IndexCore {
            indexrelid: ix.indexrelid,
            indrelid: ix.indrelid,
            indnatts: ix.indnatts,
            indnkeyatts: ix.indnkeyatts,
            indisunique: ix.indisunique,
            indnullsnotdistinct: ix.indnullsnotdistinct,
            indisprimary: ix.indisprimary,
            indisexclusion: ix.indisexclusion,
            indimmediate: ix.indimmediate,
            indisvalid: ix.indisvalid,
            indisready: ix.indisready,
            indkey: ix.indkey.iter().copied().collect(),
            has_indpred: ix.has_indpred,
            indexprs_src: ix.indexprs_src.as_ref().map(|s| Box::from(s.as_str())),
            indpred_src: ix.indpred_src.as_ref().map(|s| Box::from(s.as_str())),
            opcintype: rel.rd_opcintype.iter().copied().collect(),
            opfamily: rel.rd_opfamily.iter().copied().collect(),
            indoption: rel.rd_indoption.iter().copied().collect(),
            indcollation: rel.rd_indcollation.iter().copied().collect(),
            support: rel.rd_support.iter().copied().collect(),
            supportinfo_oids: rel
                .rd_supportinfo
                .borrow()
                .iter()
                .map(|f| f.as_ref().map(|f| f.fn_oid))
                .collect(),
        });
        Some(RelCoreShared {
            relid: rel.rd_id,
            form: rel.rd_rel.clone(),
            relhastriggers: rel.rd_hastriggers,
            relhasrules: rel.rd_hasrules,
            options: rel.rd_options.as_deref().copied(),
            tdtypeid: td.tdtypeid,
            tdtypmod: td.tdtypmod,
            attrs: td.attrs.iter().cloned().collect(),
            compact: td.compact_attrs.iter().cloned().collect(),
            constr,
            index,
        })
    }
}

// ---------------------------------------------------------------------------
// Per-thread tupdesc mirrors (the lifetime guard registry)
// ---------------------------------------------------------------------------

thread_local! {
    // core ptr -> (mirror desc, core guard). Entry removable ONLY while the
    // registry is the Rc's sole holder (strong_count == 1): every live shell
    // and every executor-held tupdesc clone pins its core through this map.
    static TD_MIRRORS: RefCell<HashMap<usize, (Rc<TupleDescData<'static>>, Arc<RelCoreShared>)>> =
        RefCell::new(HashMap::new());
    // Session-cleanup armed? Mirrors carry CacheMemoryContext allocations
    // (constr PgBox, compact copy), so the registry MUST empty during the
    // State cleanup phase — before the session-root contexts reset — never in
    // the bare TLS destructor (a free into a reset arena, found by the first
    // crash smoke). Re-armed per session (threads may be reused).
    static TD_CLEANUP_ARMED: Cell<bool> = const { Cell::new(false) };
}

const TD_MIRROR_PRUNE_LEN: usize = 256;

fn tupdesc_mirror(core: &Arc<RelCoreShared>) -> PgResult<Rc<TupleDescData<'static>>> {
    let key = Arc::as_ptr(core) as usize;
    if let Some(rc) = TD_MIRRORS.with(|m| m.borrow().get(&key).map(|(rc, _)| Rc::clone(rc))) {
        return Ok(rc);
    }
    if !TD_CLEANUP_ARMED.with(|c| c.replace(true)) {
        ::mcx::register_session_cleanup(Box::new(|| {
            TD_MIRRORS.with(|m| m.borrow_mut().clear());
            TD_CLEANUP_ARMED.with(|c| c.set(false));
        }));
    }
    let mcx = cache_mcx();
    let constr = match &core.constr {
        None => None,
        Some(c) => {
            let mut defval: PgVec<'static, AttrDefault<'static>> = PgVec::new_in(mcx);
            for (adnum, adbin) in c.defval.iter() {
                defval.push(AttrDefault {
                    adnum: *adnum,
                    // SAFETY: core-owned string; guarded by the registry entry
                    // inserted below (module doc).
                    adbin: adbin.as_deref().map(|s| unsafe { mirror_string(s) }),
                });
            }
            let mut check: PgVec<'static, ConstrCheck<'static>> = PgVec::new_in(mcx);
            for k in c.check.iter() {
                check.push(ConstrCheck {
                    // SAFETY: as above.
                    ccname: k.ccname.as_deref().map(|s| unsafe { mirror_string(s) }),
                    // SAFETY: as above.
                    ccbin: k.ccbin.as_deref().map(|s| unsafe { mirror_string(s) }),
                    ccenforced: k.ccenforced,
                    ccvalid: k.ccvalid,
                    ccnoinherit: k.ccnoinherit,
                });
            }
            Some(mcx::box_new_in(
                mcx,
                TupleConstr {
                    defval,
                    check,
                    missing: PgVec::new_in(mcx),
                    num_defval: c.num_defval,
                    num_check: c.num_check,
                    has_not_null: c.has_not_null,
                    has_generated_stored: c.has_generated_stored,
                    has_generated_virtual: c.has_generated_virtual,
                },
            ))
        }
    };
    // Per-thread COPY of the compact array (attcacheoff Cells are hot-path
    // mutable); the pg_attribute array is the shared mirror — the bulk.
    let mut compact: PgVec<'static, CompactAttribute> = PgVec::new_in(mcx);
    compact.extend(core.compact.iter().cloned());
    let td = TupleDescData {
        natts: core.attrs.len() as i32,
        tdtypeid: core.tdtypeid,
        tdtypmod: core.tdtypmod,
        tdrefcount: 1,
        constr,
        compact_attrs: compact,
        // SAFETY: core-owned array of Copy elements; guarded by the registry
        // entry inserted below.
        attrs: unsafe { mirror_vec(&core.attrs) },
    };
    let rc = Rc::new(td);
    TD_MIRRORS.with(|m| {
        let mut m = m.borrow_mut();
        if m.len() >= TD_MIRROR_PRUNE_LEN {
            m.retain(|_, (rc, _)| Rc::strong_count(rc) > 1);
        }
        m.insert(key, (Rc::clone(&rc), Arc::clone(core)));
    });
    Ok(rc)
}

// ---------------------------------------------------------------------------
// Shell assembly
// ---------------------------------------------------------------------------

/// Build a thread-local `RelationData` shell over a shared core: all
/// thread-affine state fresh (smgr, subids, pgstat, amcaches, lock info,
/// physical addr), all immutable bulk aliased from the core.
pub(crate) fn shell_from_core(core: &Arc<RelCoreShared>) -> PgResult<RelationData<'static>> {
    let mcx = cache_mcx();
    let rd_att = tupdesc_mirror(core)?;

    let (rd_index, opcintype, opfamily, indoption, indcollation, support, supportinfo) =
        match &core.index {
            Some(ix) => {
                let index = FormData_pg_index {
                    indexrelid: ix.indexrelid,
                    indrelid: ix.indrelid,
                    indnatts: ix.indnatts,
                    indnkeyatts: ix.indnkeyatts,
                    indisunique: ix.indisunique,
                    indnullsnotdistinct: ix.indnullsnotdistinct,
                    indisprimary: ix.indisprimary,
                    indisexclusion: ix.indisexclusion,
                    indimmediate: ix.indimmediate,
                    indisvalid: ix.indisvalid,
                    indisready: ix.indisready,
                    // SAFETY (all mirrors below): core-owned Copy arrays /
                    // strings; the shell's rd_att clone pins the core through
                    // the tupdesc registry for the shell's whole life.
                    indkey: unsafe { mirror_vec(&ix.indkey) },
                    has_indpred: ix.has_indpred,
                    indexprs_src: ix.indexprs_src.as_deref().map(|s| unsafe { mirror_string(s) }),
                    indpred_src: ix.indpred_src.as_deref().map(|s| unsafe { mirror_string(s) }),
                };
                let mut si: Vec<Option<types_fmgr::FmgrInfo>> =
                    Vec::with_capacity(ix.supportinfo_oids.len());
                for slot in ix.supportinfo_oids.iter() {
                    si.push(match slot {
                        Some(oid) => Some(fmgr_seams::fmgr_info::call(*oid)?),
                        None => None,
                    });
                }
                (
                    Some(index),
                    unsafe { mirror_vec(&ix.opcintype) },
                    unsafe { mirror_vec(&ix.opfamily) },
                    unsafe { mirror_vec(&ix.indoption) },
                    unsafe { mirror_vec(&ix.indcollation) },
                    unsafe { mirror_vec(&ix.support) },
                    si,
                )
            }
            None => (
                None,
                PgVec::new_in(mcx),
                PgVec::new_in(mcx),
                PgVec::new_in(mcx),
                PgVec::new_in(mcx),
                PgVec::new_in(mcx),
                Vec::new(),
            ),
        };

    RelationInitTableAccessMethod(core.relid, core.form.relkind, core.form.relam)?;

    let data = RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: core.relid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(false),
        rd_createSubid: Cell::new(InvalidSubTransactionId),
        rd_newRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
        rd_firstRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
        rd_droppedSubid: Cell::new(InvalidSubTransactionId),
        rd_lockInfo: lmgr::RelationInitLockInfo(core.relid, core.form.relisshared),
        rd_rel: core.form.clone(),
        rd_att,
        rd_index,
        rd_opcintype: opcintype,
        rd_opfamily: opfamily,
        rd_indoption: indoption,
        rd_indcollation: indcollation,
        rd_options: core.options.map(Box::new),
        pgstat_enabled: Cell::new(false),
        pgstat_link: Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: support,
        rd_supportinfo: RefCell::new(supportinfo),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: core.relhastriggers,
        rd_hasrules: core.relhasrules,
    };
    RelationInitPhysicalAddr(&data)?;
    Ok(data)
}

// ---------------------------------------------------------------------------
// The miss path
// ---------------------------------------------------------------------------

fn l2_debug() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_L2_DEBUG").is_ok_and(|v| v.trim() == "1"))
}

/// L2 gate for the relcache: kill switch, bootstrap/pre-critical phases,
/// uncommitted-DDL sessions / parallel workers / historic snapshots.
pub(crate) fn l2_active() -> bool {
    l2cache::enabled()
        && crate::with_state(|st| st.critical_relcaches_built)
        && !miscinit_seams::is_bootstrap_processing_mode::call()
        && !l2cache::private_build_mode()
}

/// `RelationIdGetRelation`'s miss arm, D3.2 form: consult the shared core
/// store before scanning the catalogs; on L2 miss, build once under the
/// per-relid gate and publish the shareable core.
pub(crate) fn miss_via_l2(relid: Oid) -> PgResult<Option<Rc<RelationData<'static>>>> {
    let db = init_small::globals::MyDatabaseId();
    let key = l2cache::L2Key { kind: l2cache::KIND_REL, id: relid, db, hash: 0 };
    let domain = l2cache::Domain::Rel(relid);
    loop {
        let gen = l2cache::view_gen(domain);
        if let Some(v) = l2cache::lookup(key, gen, |a| a.is::<RelCoreShared>()) {
            let core = v.downcast::<RelCoreShared>().expect("KIND_REL entries are RelCoreShared");
            if l2_debug() {
                eprintln!(
                    "L2DBG hit rel={} gen={} cur={} natts={} dropped={} thr={:?}",
                    relid,
                    gen,
                    l2cache::current_gen(domain),
                    core.attrs.len(),
                    core.attrs.iter().filter(|a| a.attisdropped).count(),
                    std::thread::current().id()
                );
            }
            let data = shell_from_core(&core)?;
            // The shell assembly can process invalidations (AM registration
            // takes catalog locks): if this relid's domain moved meanwhile,
            // the shell is stale at birth — discard and retry at the new view.
            if l2cache::view_gen(domain) != gen {
                continue;
            }
            let rel = Rc::new(data);
            crate::store::insert(Rc::clone(&rel), false, true)?;
            rel.rd_isvalid.set(true);
            return Ok(Some(rel));
        }
        match l2cache::acquire_gate(key, gen) {
            l2cache::GateOutcome::Waited => continue,
            // The bounded wait expired (possible undetected deadlock): fall
            // back to a private build instead of retrying forever.
            l2cache::GateOutcome::TimedOut => return crate::build::RelationBuildDesc(relid, true),
            l2cache::GateOutcome::Recursive => return crate::build::RelationBuildDesc(relid, true),
            l2cache::GateOutcome::Owner(_guard) => {
                let built = crate::build::RelationBuildDesc(relid, true)?;
                if let Some(rel) = &built {
                    // Publish only when no generation bump raced the catalog
                    // scan (same rule as the catcache path).
                    if l2cache::current_gen(domain) == gen && !l2cache::private_build_mode() {
                        if let Some(core) = RelCoreShared::from_built(rel) {
                            if l2_debug() {
                                eprintln!(
                                    "L2DBG publish rel={} gen={} natts={} thr={:?}",
                                    relid, gen, core.attrs.len(), std::thread::current().id()
                                );
                            }
                            let bytes = core.approx_bytes();
                            let v: Arc<dyn core::any::Any + Send + Sync> = Arc::new(core);
                            l2cache::insert(key, gen, v, bytes, |a| a.is::<RelCoreShared>());
                        }
                    } else if l2_debug() {
                        eprintln!(
                            "L2DBG skip-publish rel={} gen={} cur={} thr={:?}",
                            relid, gen, l2cache::current_gen(domain), std::thread::current().id()
                        );
                    }
                }
                // No pg_class row: no negative relcache caching today; the
                // L2 stays empty for this key too (deferred).
                return Ok(built);
            }
        }
    }
}

/// D3.4 idle passivation: drop registry entries whose mirror the registry is
/// the last holder of — releases the Arc core guards and the per-thread
/// compact/constr copies of shells that passivation just cleared.
pub(crate) fn prune_mirrors() {
    TD_MIRRORS.with(|m| m.borrow_mut().retain(|_, (rc, _)| Rc::strong_count(rc) > 1));
}

/// D3.2 census: this thread's mirror-registry size (shells alias shared
/// cores; per-thread cost is the registry + compact/constr copies only).
pub fn MirrorCensus() -> (usize, usize) {
    TD_MIRRORS.with(|m| {
        let m = m.borrow();
        let live = m.values().filter(|(rc, _)| Rc::strong_count(rc) > 1).count();
        (m.len(), live)
    })
}
