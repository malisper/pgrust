use core::cell::{Cell, RefCell};
use std::rc::Rc;

use ::mcx::PgVec;
use ::types_fmgr::FmgrInfo;
use ::types_nbtree::BTMetaPageData;
use ::types_core::{
    InvalidRelFileNumber, Oid, ProcNumber, SubTransactionId, BLCKSZ, RELPERSISTENCE_PERMANENT,
    RELPERSISTENCE_TEMP,
};
use ::types_error::PgResult;
use ::types_storage::smgr::SmgrHandle;
use ::types_storage::RelFileLocator;
use ::types_tuple::TupleDescData;

use crate::lock::{LockInfoData, NoLock, LOCKMODE};
use crate::pg_class::{FormData_pg_class, RELKIND_HAS_STORAGE, RELKIND_MATVIEW, RELKIND_RELATION};
use crate::pg_index::FormData_pg_index;
use crate::reloptions::RdOptions;

// RelationData (utils/rel.h) trimmed to the fields ports consume. Cell fields are
// the ones C writes through the backend-shared relcache entry pointer (inval, subxact
// tracking, pgstat arming); one backend = one thread, so Cell is the plain-store
// rendering. rd_refcnt is the Rc strong count. rd_locator/rd_smgr carry no
// lifetime (Copy payloads), keeping RelationData covariant in 'mcx.
#[derive(Debug)]
pub struct RelationData<'mcx> {
    // rd_locator: RelationInitPhysicalAddr writes; rd_smgr: only smgr's RelationGetSmgr/CloseSmgr touch (they own the pin).
    pub rd_locator: Cell<RelFileLocator>,
    pub rd_smgr: Cell<Option<SmgrHandle>>,
    pub rd_id: Oid,
    pub rd_backend: ProcNumber,
    pub rd_islocaltemp: bool,
    pub rd_isvalid: Cell<bool>,
    pub rd_createSubid: Cell<SubTransactionId>,
    pub rd_newRelfilelocatorSubid: Cell<SubTransactionId>,
    pub rd_firstRelfilelocatorSubid: Cell<SubTransactionId>,
    pub rd_droppedSubid: Cell<SubTransactionId>,
    pub rd_lockInfo: LockInfoData,
    pub rd_rel: FormData_pg_class,
    pub rd_att: Rc<TupleDescData<'mcx>>,
    pub rd_index: Option<FormData_pg_index<'mcx>>,
    pub rd_opcintype: PgVec<'mcx, Oid>,
    pub rd_opfamily: PgVec<'mcx, Oid>,
    pub rd_indoption: PgVec<'mcx, i16>,
    pub rd_indcollation: PgVec<'mcx, Oid>,
    pub rd_options: Option<RdOptions>,
    pub pgstat_enabled: Cell<bool>,
    // C rd->pgstat_info: (gen, counts) raw link into pgstat's pending relation
    // entry; dereferenceable only while gen matches pgstat's relation-pending
    // generation (count sites check before every dereference).
    pub pgstat_link: Cell<(u64, *mut ())>,
    // C rd_amcache (rule-5 cache): btree metapage copy so descents skip the read; enum when another AM lands; cleared with the entry as C pfrees it.
    pub rd_amcache: Cell<Option<RdAmCacheBtree>>,
    // C rd_amcache, hash arm (HashMetaPageData is 4.5KB - boxed, read via borrow).
    pub rd_amcache_hash: RefCell<Option<std::boxed::Box<types_hash::HashMetaPageData>>>,
    // C rd_amcache, gin arm (resolved opclass dispatch; gin crate owns the
    // tag mapping — 0 == jsonb_ops).
    pub rd_amcache_gin: Cell<Option<RdAmCacheGin>>,
    // C rd_amcache, spgist arm (SpGistCache POD: opclass config + lastUsedPages).
    pub rd_amcache_spgist: Cell<Option<types_spgist::SpGistCache>>,
    // C rd_support: nkey x amsupport support-proc OIDs, row-major.
    pub rd_support: PgVec<'mcx, Oid>,
    // C rd_support/rd_supportinfo (rule-5 cache), resolved once per column;
    // std Vec justified: Rc-owned owner structure outside the arenas, FmgrInfo is droppy.
    pub rd_supportinfo: RefCell<Vec<Option<FmgrInfo>>>,
    // C rd_opcoptions (rule-5 cache): parsed per-key-column opclass options
    // struct images, built lazily by RelationGetIndexAttOptions; Rc so AM
    // states keep the parse alive across relcache invalidation.
    pub rd_opcoptions: RefCell<Option<Rc<[Option<std::boxed::Box<[u8]>>]>>>,
    // C rd_indexlist family (rule-5 cache): None == !rd_indexvalid, inval clears it;
    // 'static (CacheMemoryContext copy, as C's) keeps RelationData covariant in 'mcx.
    pub rd_indexlist: RefCell<Option<RdIndexList>>,
    // C rd_trigdesc (rule-5 cache): Rc replaces C's CopyTriggerDesc per-query
    // deep copy; inval drops the entry's Rc, executors keep theirs.
    pub rd_trigdesc: RefCell<Option<Rc<types_trigger::TriggerDesc<'static>>>>,
    // pg_class.relhastriggers, threaded beside the trimmed rd_rel form
    // (ScannedPgClass.relchecks precedent).
    pub rd_hastriggers: bool,
    // pg_class.relhasrules, same threading (matchLocks/fireRIRrules gate).
    pub rd_hasrules: bool,
}

#[derive(Debug)]
pub struct RdIndexList {
    pub list: PgVec<'static, Oid>,
    pub pkindex: Oid,
    pub ispkdeferrable: bool,
    pub replidindex: Oid,
}

pub type RdAmCacheBtree = BTMetaPageData;

/// GIN's resolved per-column opclass state (gin's GinColState mirror).
#[derive(Clone, Copy, Debug)]
pub struct RdAmCacheGinCol {
    pub opclass: u8,
    /// array_ops element comparator tag (gin's GinElemCmp mirror).
    pub elem_cmp: u8,
    pub support_collation: Oid,
    pub can_partial_match: bool,
    pub key_byval: bool,
    pub key_len: i16,
}

/// GIN's resolved opclass state (gin's GinState mirror; INDEX_MAX_KEYS slots).
#[derive(Clone, Copy, Debug)]
pub struct RdAmCacheGin {
    pub natts: u16,
    pub cols: [RdAmCacheGinCol; 32],
}

impl<'mcx> RelationData<'mcx> {
    #[inline]
    pub fn descr(&self) -> &TupleDescData<'mcx> {
        &self.rd_att
    }

    #[inline]
    pub fn name(&self) -> &str {
        core::str::from_utf8(self.rd_rel.relname.name_str()).expect("non-UTF-8 relname")
    }

    #[inline]
    pub fn namespace(&self) -> Oid {
        self.rd_rel.relnamespace
    }

    #[inline]
    pub fn indnatts(&self) -> i32 {
        self.rd_index.as_ref().map_or(0, |i| i.indnatts as i32)
    }

    #[inline]
    pub fn indnkeyatts(&self) -> i32 {
        self.rd_index.as_ref().map_or(0, |i| i.indnkeyatts as i32)
    }

    #[inline]
    pub fn is_scannable(&self) -> bool {
        self.rd_rel.relispopulated
    }

    #[inline]
    pub fn is_permanent(&self) -> bool {
        self.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT
    }

    #[inline]
    pub fn uses_local_buffers(&self) -> bool {
        self.rd_rel.relpersistence == RELPERSISTENCE_TEMP
    }

    /// C's `RELATION_IS_OTHER_TEMP` (rel.h:669-671): a temporary relation
    /// belonging to some *other* session. Its pages live in that backend's
    /// local buffers, so this backend can neither read them coherently nor
    /// write them safely. `resolve_backend` (relcache build) really does hand
    /// back a foreign proc number with `rd_islocaltemp == false`, so this is
    /// reachable-true — it is not a const-false predicate.
    #[inline]
    pub fn is_other_temp(&self) -> bool {
        self.rd_rel.relpersistence == RELPERSISTENCE_TEMP && !self.rd_islocaltemp
    }

    #[inline]
    pub fn is_mapped(&self) -> bool {
        RELKIND_HAS_STORAGE(self.rd_rel.relkind)
            && self.rd_rel.relfilenode == InvalidRelFileNumber
    }

    #[inline]
    pub fn get_fillfactor(&self, defaultff: i32) -> i32 {
        match self.rd_options.as_ref().and_then(|o| o.fillfactor()) {
            Some(ff) => ff,
            None => defaultff,
        }
    }

    #[inline]
    pub fn get_target_page_usage(&self, defaultff: i32) -> usize {
        BLCKSZ * self.get_fillfactor(defaultff) as usize / 100
    }

    #[inline]
    pub fn get_target_page_free_space(&self, defaultff: i32) -> usize {
        BLCKSZ * (100 - self.get_fillfactor(defaultff)) as usize / 100
    }

    #[inline]
    pub fn get_toast_tuple_target(&self, defaulttarg: i32) -> i32 {
        match self.rd_options.as_ref().and_then(|o| o.std()) {
            Some(opts) => opts.toast_tuple_target,
            None => defaulttarg,
        }
    }

    #[inline]
    pub fn get_parallel_workers(&self, defaultpw: i32) -> i32 {
        // C's RelationGetParallelWorkers reads StdRdOptions; pgrcolumnar carries
        // the same option in its own parse struct (same -1 = unset contract).
        match self.rd_options.as_ref() {
            Some(o) => match o {
                crate::reloptions::RdOptions::Std(opts) => opts.parallel_workers,
                crate::reloptions::RdOptions::Pgrcolumnar(opts) => opts.parallel_workers,
                _ => defaultpw,
            },
            None => defaultpw,
        }
    }

    #[inline]
    pub fn is_used_as_catalog_table(&self) -> bool {
        (self.rd_rel.relkind == RELKIND_RELATION || self.rd_rel.relkind == RELKIND_MATVIEW)
            && self
                .rd_options
                .as_ref()
                .and_then(|o| o.std())
                .is_some_and(|o| o.user_catalog_table)
    }
}

pub type RelationCloser = fn(Oid, LOCKMODE) -> PgResult<()>;

// C `Relation`: a pointer to the refcounted relcache entry. Rc is C's own
// refcount (rd_refcnt == strong count); only the opening handle carries the
// close (Drop = C's abort-path relation_close(rel, NoLock); lock release
// belongs to transaction cleanup). alias() is C's pointer alias: no authority.
pub struct Relation<'mcx> {
    data: Rc<RelationData<'mcx>>,
    closer: Option<RelationCloser>,
}

impl<'mcx> Relation<'mcx> {
    pub fn open(data: RelationData<'mcx>, closer: Option<RelationCloser>) -> Self {
        Relation {
            data: Rc::new(data),
            closer,
        }
    }

    pub fn open_rc(data: Rc<RelationData<'mcx>>, closer: Option<RelationCloser>) -> Self {
        Relation { data, closer }
    }

    pub fn alias(&self) -> Relation<'mcx> {
        Relation {
            data: Rc::clone(&self.data),
            closer: None,
        }
    }

    pub fn data_rc(&self) -> &Rc<RelationData<'mcx>> {
        &self.data
    }

    pub fn close(mut self, lockmode: LOCKMODE) -> PgResult<()> {
        match self.closer.take() {
            Some(closer) => closer(self.data.rd_id, lockmode),
            None => Ok(()),
        }
    }

    pub fn disarm_closer(&mut self) {
        let _ = self.closer.take();
    }
}

impl<'mcx> core::ops::Deref for Relation<'mcx> {
    type Target = RelationData<'mcx>;

    fn deref(&self) -> &RelationData<'mcx> {
        &self.data
    }
}

impl core::fmt::Debug for Relation<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Relation")
            .field("data", &self.data)
            .field("has_closer", &self.closer.is_some())
            .finish()
    }
}

impl Drop for Relation<'_> {
    fn drop(&mut self) {
        if let Some(closer) = self.closer.take() {
            // C's abort-path close has no error surface. If this Drop runs
            // during an unwind and the closer panics (e.g. an unported seam),
            // a second panic would abort the backend — swallow it.
            let id = self.data.rd_id;
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = closer(id, NoLock);
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_class::REPLICA_IDENTITY_DEFAULT;
    use crate::reloptions::{
        AutoVacOpts, RdOptions, StdRdOptions, HEAP_DEFAULT_FILLFACTOR,
        STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
    };
    use ::mcx::MemoryContext;
    use ::types_core::INVALID_PROC_NUMBER;
    use ::types_tuple::NameData;
    use core::sync::atomic::{AtomicU32, Ordering};

    fn form_pg_class(oid: Oid) -> FormData_pg_class {
        let mut relname = NameData::default();
        relname.namestrcpy("t");
        FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: 2,
            relfilenode: oid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        }
    }

    fn rel_data<'mcx>(mcx: ::mcx::Mcx<'mcx>, oid: Oid) -> RelationData<'mcx> {
        let td = TupleDescData {
            natts: 0,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs: PgVec::new_in(mcx),
            attrs: PgVec::new_in(mcx),
        };
        RelationData {
            rd_locator: Default::default(),
            rd_smgr: Default::default(),
            rd_id: oid,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: Cell::new(true),
            rd_createSubid: Cell::new(0),
            rd_newRelfilelocatorSubid: Cell::new(0),
            rd_firstRelfilelocatorSubid: Cell::new(0),
            rd_droppedSubid: Cell::new(0),
            rd_lockInfo: LockInfoData {
                lockRelId: crate::lock::LockRelId { relId: oid, dbId: 5 },
            },
            rd_rel: form_pg_class(oid),
            rd_att: Rc::new(td),
            rd_index: None,
            rd_opcintype: PgVec::new_in(mcx),
            rd_opfamily: PgVec::new_in(mcx),
            rd_indoption: PgVec::new_in(mcx),
            rd_indcollation: PgVec::new_in(mcx),
            rd_options: None,
            pgstat_enabled: Cell::new(false),
            pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(),
            rd_amcache_gin: Default::default(),
            rd_amcache_spgist: Default::default(),
            rd_support: PgVec::new_in(mcx),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
        }
    }

    fn std_options(fillfactor: i32) -> StdRdOptions {
        StdRdOptions {
            fillfactor,
            toast_tuple_target: 2032,
            autovacuum: AutoVacOpts {
                enabled: true,
                vacuum_threshold: 50,
                vacuum_max_threshold: -1,
                vacuum_ins_threshold: 1000,
                analyze_threshold: 50,
                vacuum_cost_limit: -1,
                freeze_min_age: -1,
                freeze_max_age: -1,
                freeze_table_age: -1,
                multixact_freeze_min_age: -1,
                multixact_freeze_max_age: -1,
                multixact_freeze_table_age: -1,
                log_min_duration: -1,
                vacuum_cost_delay: -1.0,
                vacuum_scale_factor: 0.2,
                vacuum_ins_scale_factor: 0.2,
                analyze_scale_factor: 0.1,
            },
            user_catalog_table: false,
            parallel_workers: -1,
            vacuum_index_cleanup: STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
            vacuum_truncate: true,
            vacuum_truncate_set: false,
            vacuum_max_eager_freeze_failure_rate: -1.0,
        }
    }

    #[test]
    fn lockmodes_match_lockdefs_h() {
        assert_eq!(NoLock, 0);
        assert_eq!(crate::lock::AccessShareLock, 1);
        assert_eq!(crate::lock::RowShareLock, 2);
        assert_eq!(crate::lock::RowExclusiveLock, 3);
        assert_eq!(crate::lock::ShareUpdateExclusiveLock, 4);
        assert_eq!(crate::lock::ShareLock, 5);
        assert_eq!(crate::lock::ShareRowExclusiveLock, 6);
        assert_eq!(crate::lock::ExclusiveLock, 7);
        assert_eq!(crate::lock::AccessExclusiveLock, 8);
        assert_eq!(crate::lock::MaxLockMode, 8);
        assert_eq!(crate::lock::InplaceUpdateTupleLock, 7);
    }

    #[test]
    fn rel_macros_match_rel_h() {
        let ctx = MemoryContext::new("test");
        let mut rel = rel_data(ctx.mcx(), 16384);

        assert_eq!(rel.name(), "t");
        assert_eq!(rel.namespace(), 2200);
        assert!(rel.is_scannable());
        assert!(rel.is_permanent());
        assert!(!rel.uses_local_buffers());
        assert!(!rel.is_mapped());
        assert_eq!(rel.indnkeyatts(), 0);

        assert_eq!(rel.get_fillfactor(HEAP_DEFAULT_FILLFACTOR), 100);
        assert_eq!(rel.get_target_page_free_space(HEAP_DEFAULT_FILLFACTOR), 0);
        rel.rd_options = Some(RdOptions::Std(std_options(70)));
        assert_eq!(rel.get_fillfactor(HEAP_DEFAULT_FILLFACTOR), 70);
        assert_eq!(rel.get_target_page_usage(HEAP_DEFAULT_FILLFACTOR), BLCKSZ * 70 / 100);
        assert_eq!(
            rel.get_target_page_free_space(HEAP_DEFAULT_FILLFACTOR),
            BLCKSZ * 30 / 100
        );
        assert_eq!(rel.get_toast_tuple_target(2032), 2032);
        assert_eq!(rel.get_parallel_workers(-1), -1);
        assert!(!rel.is_used_as_catalog_table());

        rel.rd_rel.relfilenode = InvalidRelFileNumber;
        assert!(rel.is_mapped());
        rel.rd_rel.relpersistence = RELPERSISTENCE_TEMP;
        assert!(rel.uses_local_buffers());
        assert!(!rel.is_permanent());
    }

    static CLOSES: AtomicU32 = AtomicU32::new(0);

    fn counting_closer(_relid: Oid, _mode: LOCKMODE) -> PgResult<()> {
        CLOSES.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[test]
    fn handle_release_semantics() {
        let ctx = MemoryContext::new("test");
        CLOSES.store(0, Ordering::Relaxed);

        let rel = Relation::open(rel_data(ctx.mcx(), 1), Some(counting_closer));
        assert_eq!(Rc::strong_count(rel.data_rc()), 1);
        let a = rel.alias();
        assert_eq!(Rc::strong_count(rel.data_rc()), 2);
        assert_eq!(a.rd_id, 1);
        drop(a);
        assert_eq!(Rc::strong_count(rel.data_rc()), 1);
        assert_eq!(CLOSES.load(Ordering::Relaxed), 0);

        rel.close(crate::lock::AccessShareLock).unwrap();
        assert_eq!(CLOSES.load(Ordering::Relaxed), 1);

        let rel = Relation::open(rel_data(ctx.mcx(), 2), Some(counting_closer));
        drop(rel);
        assert_eq!(CLOSES.load(Ordering::Relaxed), 2);

        let mut rel = Relation::open(rel_data(ctx.mcx(), 3), Some(counting_closer));
        rel.disarm_closer();
        drop(rel);
        assert_eq!(CLOSES.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn shared_projection_open_shares_without_copy() {
        let ctx = MemoryContext::new("test");
        let cached = Rc::new(rel_data(ctx.mcx(), 9));
        let rel = Relation::open_rc(Rc::clone(&cached), None);
        assert_eq!(Rc::strong_count(&cached), 2);
        assert!(Rc::ptr_eq(rel.data_rc(), &cached));
        rel.pgstat_enabled.set(true);
        assert!(cached.pgstat_enabled.get());
        drop(rel);
        assert_eq!(Rc::strong_count(&cached), 1);
    }
}
