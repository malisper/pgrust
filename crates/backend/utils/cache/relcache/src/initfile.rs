use std::path::{Path, PathBuf};
use std::rc::Rc;

use mcx::{Mcx, MemoryContext, PgString, PgVec};
use types_core::{InvalidSubTransactionId, Oid, RECORDOID};
use types_error::{PgError, PgResult, DEBUG1, ERRCODE_INTERNAL_ERROR, WARNING};
use types_rel::{FormData_pg_class, FormData_pg_index, RelationData, RELKIND_INDEX};
use types_rel::{
    AutoVacOpts, BTOptions, BrinOptions, GinOptions, GistOptions, HashOptions, RdOptions,
    SpGistOptions, StdRdOptions, ViewOptions, GIST_OPTION_BUFFERING_AUTO,
    GIST_OPTION_BUFFERING_OFF, GIST_OPTION_BUFFERING_ON,
    STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO, STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF,
    STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON, VIEW_OPTION_CHECK_OPTION_CASCADED,
    VIEW_OPTION_CHECK_OPTION_LOCAL, VIEW_OPTION_CHECK_OPTION_NOT_SET,
};
use types_tuple::{FormData_pg_attribute, NameData, TupleConstr};

use crate::schemapg::{
    ACCESS_METHOD_PROCEDURE_INDEX_ID, ACCESS_METHOD_PROCEDURE_RELATION_ID,
    ATTRIBUTE_RELID_NUM_INDEX_ID, AUTH_ID_OID_INDEX_ID, AUTH_ID_ROLNAME_INDEX_ID,
    AUTH_MEM_MEM_ROLE_INDEX_ID, CAT_PG_SHSECLABEL, CLASS_OID_INDEX_ID, DATABASE_NAME_INDEX_ID,
    DATABASE_OID_INDEX_ID, INDEX_RELID_INDEX_ID, LOCAL_BOOTSTRAP_CATALOGS,
    OPCLASS_OID_INDEX_ID, OPERATOR_CLASS_RELATION_ID, REWRITE_RELATION_ID,
    REWRITE_REL_RULENAME_INDEX_ID, SHARED_BOOTSTRAP_CATALOGS, SHARED_SEC_LABEL_OBJECT_INDEX_ID,
    TRIGGER_RELATION_ID, TRIGGER_RELID_NAME_INDEX_ID,
};
use crate::{build, store, with_state};
use types_rel::AccessShareLock;

// Divergence from C: our file is a different on-disk format, so it lives under
// a different name; C 18.3 writes/reads its own pg_internal.init in the same
// datadir and neither binary ever opens the other's file. Our unlink paths
// remove BOTH names so a later C boot cannot trust a file our DDL made stale.
pub const RELCACHE_INIT_FILENAME: &str = "pgrust_internal.init";
pub const C_RELCACHE_INIT_FILENAME: &str = "pg_internal.init";
pub const RELCACHE_INIT_FILEMAGIC: i32 = 0x573266;
// Bump whenever the entry codec below changes shape; a mismatch rejects the file.
pub const RELCACHE_INIT_FORMAT: u32 = 1;
const TABLESPACE_VERSION_DIRECTORY: &str = "PG_18_202506291";
const PG_TBLSPC_DIR: &str = "pg_tblspc";
// BUILTIN_TRANCHE_NAMES[16] == "RelCacheInit"; pinned by a test.
const RELCACHE_INIT_LOCK_OFFSET: usize = 16;

const NUM_CRITICAL_SHARED_RELS: usize = SHARED_BOOTSTRAP_CATALOGS.len();
const NUM_CRITICAL_LOCAL_RELS: usize = LOCAL_BOOTSTRAP_CATALOGS.len();
const NUM_CRITICAL_LOCAL_INDEXES: usize = 7;
const NUM_CRITICAL_SHARED_INDEXES: usize = 6;

const BTREE_AM_OID: Oid = 403;
const HASH_AM_OID: Oid = 405;
const MAX_ATTS: usize = 1600;


// PGRUST_GATHER_TRACE sub-attribution of RelationCacheInitializePhase3's
// cost (the §2 table's 2.1ms line): which of file-load / critical-index /
// finish / warm+write burns it decides the P-share-lite retention shape.
fn gtrace_us(label: &str, t0: std::time::Instant) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if !*ON.get_or_init(|| std::env::var_os("PGRUST_GATHER_TRACE").is_some()) {
        return;
    }
    eprintln!("GTRACE relcache3.{label} dt_us={}", t0.elapsed().as_micros());
}

pub fn RelationCacheInitialize() {
    // The hash table is created eagerly by the state cell (INITRELCACHESIZE).
    with_state(|_| ());
    relmapper_seams::relation_map_initialize::call();
}

pub fn RelationCacheInitializePhase2() -> PgResult<()> {
    relmapper_seams::relation_map_initialize_phase2::call()?;
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }
    if !load_relcache_init_file(true)? {
        for cat in SHARED_BOOTSTRAP_CATALOGS {
            build::formrdesc(cat)?;
        }
    }
    Ok(())
}

pub fn RelationCacheInitializePhase3() -> PgResult<()> {
    let mut need_new_cache_file = !with_state(|st| st.critical_shared_relcaches_built);
    relmapper_seams::relation_map_initialize_phase3::call()?;

    let bootstrap = miscinit_seams::is_bootstrap_processing_mode::call();
    let t0 = std::time::Instant::now();
    if bootstrap || !load_relcache_init_file(false)? {
        need_new_cache_file = true;
        for cat in LOCAL_BOOTSTRAP_CATALOGS {
            build::formrdesc(cat)?;
        }
        gtrace_us("formrdesc", t0);
    } else {
        gtrace_us("initfile_load", t0);
    }
    if bootstrap {
        return Ok(());
    }
    let t0 = std::time::Instant::now();

    // Critical indexes break the relcache-load recursion: until they're
    // nailed, ScanPgRelation heapscans (criticalRelcachesBuilt gates index_ok).
    if !crate::criticalRelcachesBuilt() {
        load_critical_index(CLASS_OID_INDEX_ID, types_core::RELATION_RELATION_ID)?;
        load_critical_index(ATTRIBUTE_RELID_NUM_INDEX_ID, types_core::ATTRIBUTE_RELATION_ID)?;
        load_critical_index(INDEX_RELID_INDEX_ID, types_core::INDEX_RELATION_ID)?;
        load_critical_index(OPCLASS_OID_INDEX_ID, OPERATOR_CLASS_RELATION_ID)?;
        load_critical_index(ACCESS_METHOD_PROCEDURE_INDEX_ID, ACCESS_METHOD_PROCEDURE_RELATION_ID)?;
        load_critical_index(REWRITE_REL_RULENAME_INDEX_ID, REWRITE_RELATION_ID)?;
        load_critical_index(TRIGGER_RELID_NAME_INDEX_ID, TRIGGER_RELATION_ID)?;
        with_state(|st| st.critical_relcaches_built = true);
    }

    if !crate::criticalSharedRelcachesBuilt() {
        load_critical_index(DATABASE_NAME_INDEX_ID, types_core::DATABASE_RELATION_ID)?;
        load_critical_index(DATABASE_OID_INDEX_ID, types_core::DATABASE_RELATION_ID)?;
        load_critical_index(AUTH_ID_ROLNAME_INDEX_ID, types_core::AUTH_ID_RELATION_ID)?;
        load_critical_index(AUTH_ID_OID_INDEX_ID, types_core::AUTH_ID_RELATION_ID)?;
        load_critical_index(AUTH_MEM_MEM_ROLE_INDEX_ID, types_core::AUTH_MEM_RELATION_ID)?;
        load_critical_index(SHARED_SEC_LABEL_OBJECT_INDEX_ID, CAT_PG_SHSECLABEL.relid)?;
        with_state(|st| st.critical_shared_relcaches_built = true);
    }

    gtrace_us("critical_indexes", t0);

    let t0 = std::time::Instant::now();
    finish_relcache_entries()?;
    gtrace_us("finish_entries", t0);

    let t0 = std::time::Instant::now();
    if need_new_cache_file {
        // Without this pre-warm the lazy relcache builds of syscache
        // catalogs land inside the first planning window and EXPLAIN
        // (BUFFERS) Planning counts diverge from C (+20-26 touches).
        syscache_seams::init_catalog_cache_phase2::call()?;
        write_relcache_init_file(true)?;
        write_relcache_init_file(false)?;
        gtrace_us("warm_and_write", t0);
    }
    Ok(())
}

// Replace formrdesc stubs (relowner == InvalidOid) with the real pg_class
// row. C also refreshes rules/triggers/RLS/tableam here; those fields live
// with later units. Restart-from-scratch scan shape as in C.
fn finish_relcache_entries() -> PgResult<()> {
    loop {
        let target = with_state(|st| {
            st.id_cache
                .iter()
                .find(|(_, e)| e.rel.rd_rel.relowner == types_core::InvalidOid)
                .map(|(k, e)| (*k, std::rc::Rc::clone(&e.rel)))
        });
        let Some((relid, rel)) = target else {
            return Ok(());
        };
        let index_ok = crate::criticalRelcachesBuilt();
        let scanned = relcache_build_seams::scan_pg_relation::call(relid, index_ok, false)?
            .ok_or_else(|| cache_lookup_failed(relid))?;
        debug_assert_eq!(rel.rd_att.tdtypeid, scanned.form.reltype);
        debug_assert_eq!(rel.rd_att.tdtypmod, -1);
        if scanned.form.relowner == types_core::InvalidOid {
            return Err(Box::new(
                PgError::error(format!(
                    "invalid relowner in pg_class entry for \"{}\"",
                    rel.name()
                ))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
            ));
        }
        // formrdesc set up rd_att correctly by construction (C asserts, never
        // copies it: catcache entries may already share it).
        let newrel = std::rc::Rc::new(types_rel::RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
            rd_id: relid,
            rd_backend: rel.rd_backend,
            rd_islocaltemp: rel.rd_islocaltemp,
            rd_isvalid: core::cell::Cell::new(true),
            rd_createSubid: core::cell::Cell::new(types_core::InvalidSubTransactionId),
            rd_newRelfilelocatorSubid: core::cell::Cell::new(types_core::InvalidSubTransactionId),
            rd_firstRelfilelocatorSubid: core::cell::Cell::new(types_core::InvalidSubTransactionId),
            rd_droppedSubid: core::cell::Cell::new(types_core::InvalidSubTransactionId),
            rd_lockInfo: lmgr::RelationInitLockInfo(relid, scanned.form.relisshared),
            rd_rel: scanned.form,
            rd_att: std::rc::Rc::clone(&rel.rd_att),
            rd_index: None,
            rd_opcintype: mcx::PgVec::new_in(crate::cache_mcx()),
            rd_opfamily: mcx::PgVec::new_in(crate::cache_mcx()),
            rd_indoption: mcx::PgVec::new_in(crate::cache_mcx()),
            rd_indcollation: mcx::PgVec::new_in(crate::cache_mcx()),
            rd_options: scanned.options,
            pgstat_enabled: core::cell::Cell::new(rel.pgstat_enabled.get()),
            // C SWAPFIELD keeps pgstat_info across the rebuild; same key, gen
            // still governs validity.
            pgstat_link: core::cell::Cell::new(rel.pgstat_link.get()),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
            rd_support: mcx::PgVec::new_in(crate::cache_mcx()),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
        });
        crate::build::RelationInitPhysicalAddr(&newrel)?;
        with_state(|st| {
            if let Some(ent) = st.id_cache.get_mut(&relid) {
                ent.rel = std::rc::Rc::clone(&newrel);
            }
        });
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn cache_lookup_failed(relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cache lookup failed for relation {relid}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

fn load_critical_index(indexoid: Oid, heapoid: Oid) -> PgResult<()> {
    // Catalog before index, or deadlock against exclusive lockers.
    lmgr::LockRelationOid(heapoid, AccessShareLock)?;
    lmgr::LockRelationOid(indexoid, AccessShareLock)?;
    let ird = build::RelationBuildDesc(indexoid, true)?;
    if ird.is_none() {
        return Err(Box::new(
            PgError::error(format!("could not open critical system index {indexoid}"))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        ));
    }
    // C: rd_isnailed = true, rd_refcnt = 1 (the nail is the flag here).
    with_state(|st| {
        if let Some(ent) = st.id_cache.get_mut(&indexoid) {
            ent.nailed = true;
        }
    });
    lmgr::UnlockRelationOid(indexoid, AccessShareLock)?;
    lmgr::UnlockRelationOid(heapoid, AccessShareLock)?;
    // C also pre-warms RelationGetIndexAttOptions (derived-data unit).
    Ok(())
}

fn init_file_path(shared: bool) -> Option<PathBuf> {
    if shared {
        Some(Path::new("global").join(RELCACHE_INIT_FILENAME))
    } else {
        init_small::globals::DatabasePath()
            .map(|p| Path::new(p).join(RELCACHE_INIT_FILENAME))
    }
}

struct Rd<'a> {
    b: &'a [u8],
    off: usize,
}

impl<'a> Rd<'a> {
    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.off.checked_add(n)?;
        let s = self.b.get(self.off..end)?;
        self.off = end;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.bytes(1)?[0])
    }
    fn boolean(&mut self) -> Option<bool> {
        match self.u8()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }
    fn i8(&mut self) -> Option<i8> {
        Some(self.u8()? as i8)
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_ne_bytes(self.bytes(2)?.try_into().ok()?))
    }
    fn i16(&mut self) -> Option<i16> {
        Some(self.u16()? as i16)
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_ne_bytes(self.bytes(4)?.try_into().ok()?))
    }
    fn i32(&mut self) -> Option<i32> {
        Some(self.u32()? as i32)
    }
    fn f32(&mut self) -> Option<f32> {
        Some(f32::from_bits(self.u32()?))
    }
    fn f64(&mut self) -> Option<f64> {
        Some(f64::from_bits(u64::from_ne_bytes(self.bytes(8)?.try_into().ok()?)))
    }
    fn at_end(&self) -> bool {
        self.off == self.b.len()
    }
}

pub(crate) type Buf<'mcx> = PgVec<'mcx, u8>;

fn put_u8(buf: &mut Buf<'_>, v: u8) {
    buf.push(v);
}
fn put_bool(buf: &mut Buf<'_>, v: bool) {
    buf.push(v as u8);
}
fn put_i8(buf: &mut Buf<'_>, v: i8) {
    buf.push(v as u8);
}
fn put_u16(buf: &mut Buf<'_>, v: u16) {
    buf.extend_from_slice(&v.to_ne_bytes());
}
fn put_i16(buf: &mut Buf<'_>, v: i16) {
    buf.extend_from_slice(&v.to_ne_bytes());
}
fn put_u32(buf: &mut Buf<'_>, v: u32) {
    buf.extend_from_slice(&v.to_ne_bytes());
}
fn put_i32(buf: &mut Buf<'_>, v: i32) {
    buf.extend_from_slice(&v.to_ne_bytes());
}
fn put_f32(buf: &mut Buf<'_>, v: f32) {
    put_u32(buf, v.to_bits());
}
fn put_f64(buf: &mut Buf<'_>, v: f64) {
    buf.extend_from_slice(&v.to_bits().to_ne_bytes());
}

fn put_class(buf: &mut Buf<'_>, f: &FormData_pg_class) {
    buf.extend_from_slice(&f.relname.data);
    put_u32(buf, f.relnamespace);
    put_u32(buf, f.reltype);
    put_u32(buf, f.relowner);
    put_u32(buf, f.relam);
    put_u32(buf, f.relfilenode);
    put_u32(buf, f.reltablespace);
    put_i32(buf, f.relpages);
    put_f32(buf, f.reltuples);
    put_i32(buf, f.relallvisible);
    put_u32(buf, f.reltoastrelid);
    put_bool(buf, f.relhasindex);
    put_bool(buf, f.relisshared);
    put_u8(buf, f.relpersistence);
    put_u8(buf, f.relkind);
    put_bool(buf, f.relhassubclass);
    put_bool(buf, f.relrowsecurity);
    put_bool(buf, f.relispopulated);
    put_u8(buf, f.relreplident);
    put_bool(buf, f.relispartition);
    put_u32(buf, f.relfrozenxid);
    put_u32(buf, f.relminmxid);
}

fn parse_class(rd: &mut Rd<'_>) -> Option<FormData_pg_class> {
    Some(FormData_pg_class {
        relname: NameData { data: rd.bytes(64)?.try_into().ok()? },
        relnamespace: rd.u32()?,
        reltype: rd.u32()?,
        relowner: rd.u32()?,
        relam: rd.u32()?,
        relfilenode: rd.u32()?,
        reltablespace: rd.u32()?,
        relpages: rd.i32()?,
        reltuples: rd.f32()?,
        relallvisible: rd.i32()?,
        reltoastrelid: rd.u32()?,
        relhasindex: rd.boolean()?,
        relisshared: rd.boolean()?,
        relpersistence: rd.u8()?,
        relkind: rd.u8()?,
        relhassubclass: rd.boolean()?,
        relrowsecurity: rd.boolean()?,
        relispopulated: rd.boolean()?,
        relreplident: rd.u8()?,
        relispartition: rd.boolean()?,
        relfrozenxid: rd.u32()?,
        relminmxid: rd.u32()?,
    })
}

fn put_attr(buf: &mut Buf<'_>, a: &FormData_pg_attribute) {
    put_u32(buf, a.attrelid);
    buf.extend_from_slice(&a.attname.data);
    put_u32(buf, a.atttypid);
    put_i16(buf, a.attlen);
    put_i16(buf, a.attnum);
    put_i32(buf, a.atttypmod);
    put_i16(buf, a.attndims);
    put_bool(buf, a.attbyval);
    put_i8(buf, a.attalign);
    put_i8(buf, a.attstorage);
    put_i8(buf, a.attcompression);
    put_bool(buf, a.attnotnull);
    put_bool(buf, a.atthasdef);
    put_bool(buf, a.atthasmissing);
    put_i8(buf, a.attidentity);
    put_i8(buf, a.attgenerated);
    put_bool(buf, a.attisdropped);
    put_bool(buf, a.attislocal);
    put_i16(buf, a.attinhcount);
    put_u32(buf, a.attcollation);
}

fn parse_attr(rd: &mut Rd<'_>) -> Option<FormData_pg_attribute> {
    Some(FormData_pg_attribute {
        attrelid: rd.u32()?,
        attname: NameData { data: rd.bytes(64)?.try_into().ok()? },
        atttypid: rd.u32()?,
        attlen: rd.i16()?,
        attnum: rd.i16()?,
        atttypmod: rd.i32()?,
        attndims: rd.i16()?,
        attbyval: rd.boolean()?,
        attalign: rd.i8()?,
        attstorage: rd.i8()?,
        attcompression: rd.i8()?,
        attnotnull: rd.boolean()?,
        atthasdef: rd.boolean()?,
        atthasmissing: rd.boolean()?,
        attidentity: rd.i8()?,
        attgenerated: rd.i8()?,
        attisdropped: rd.boolean()?,
        attislocal: rd.boolean()?,
        attinhcount: rd.i16()?,
        attcollation: rd.u32()?,
    })
}

fn put_options(buf: &mut Buf<'_>, o: &Option<RdOptions>) {
    match o {
        None => put_u8(buf, 0),
        Some(RdOptions::Std(s)) => {
            put_u8(buf, 1);
            put_i32(buf, s.fillfactor);
            put_i32(buf, s.toast_tuple_target);
            let a = &s.autovacuum;
            put_bool(buf, a.enabled);
            put_i32(buf, a.vacuum_threshold);
            put_i32(buf, a.vacuum_max_threshold);
            put_i32(buf, a.vacuum_ins_threshold);
            put_i32(buf, a.analyze_threshold);
            put_i32(buf, a.vacuum_cost_limit);
            put_i32(buf, a.freeze_min_age);
            put_i32(buf, a.freeze_max_age);
            put_i32(buf, a.freeze_table_age);
            put_i32(buf, a.multixact_freeze_min_age);
            put_i32(buf, a.multixact_freeze_max_age);
            put_i32(buf, a.multixact_freeze_table_age);
            put_i32(buf, a.log_min_duration);
            put_f64(buf, a.vacuum_cost_delay);
            put_f64(buf, a.vacuum_scale_factor);
            put_f64(buf, a.vacuum_ins_scale_factor);
            put_f64(buf, a.analyze_scale_factor);
            put_bool(buf, s.user_catalog_table);
            put_i32(buf, s.parallel_workers);
            put_i32(buf, s.vacuum_index_cleanup as i32);
            put_bool(buf, s.vacuum_truncate);
            put_bool(buf, s.vacuum_truncate_set);
            put_f64(buf, s.vacuum_max_eager_freeze_failure_rate);
        }
        Some(RdOptions::View(v)) => {
            put_u8(buf, 2);
            put_bool(buf, v.security_barrier);
            put_bool(buf, v.security_invoker);
            put_i32(buf, v.check_option as i32);
        }
        Some(RdOptions::BTree(o)) => {
            put_u8(buf, 3);
            put_i32(buf, o.fillfactor);
            put_f64(buf, o.vacuum_cleanup_index_scale_factor);
            put_bool(buf, o.deduplicate_items);
        }
        Some(RdOptions::Hash(o)) => {
            put_u8(buf, 4);
            put_i32(buf, o.fillfactor);
        }
        Some(RdOptions::Gin(o)) => {
            put_u8(buf, 5);
            put_bool(buf, o.use_fast_update);
            put_i32(buf, o.pending_list_cleanup_size);
        }
        Some(RdOptions::Gist(o)) => {
            put_u8(buf, 6);
            put_i32(buf, o.fillfactor);
            put_i32(buf, o.buffering_mode as i32);
        }
        Some(RdOptions::SpGist(o)) => {
            put_u8(buf, 7);
            put_i32(buf, o.fillfactor);
        }
        Some(RdOptions::Brin(o)) => {
            put_u8(buf, 8);
            put_i32(buf, o.pages_per_range);
            put_bool(buf, o.autosummarize);
        }
        Some(RdOptions::Pgrcolumnar(o)) => {
            put_u8(buf, 9);
            put_i32(
                buf,
                match o.codec {
                    ::types_rel::PgrcolumnarCodec::Auto => 0,
                    ::types_rel::PgrcolumnarCodec::Lz4 => 1,
                    ::types_rel::PgrcolumnarCodec::Zstd => 2,
                    ::types_rel::PgrcolumnarCodec::Plain => 3,
                },
            );
            put_i32(buf, o.zstd_level);
            let ck = o.cluster_key().as_bytes();
            put_u16(buf, ck.len() as u16);
            buf.extend_from_slice(ck);
            let cc = o.codec_cols().as_bytes();
            put_u16(buf, cc.len() as u16);
            buf.extend_from_slice(cc);
        }
        Some(RdOptions::Hnsw(o)) => {
            put_u8(buf, 10);
            put_i32(buf, o.m);
            put_i32(buf, o.ef_construction);
        }
        Some(RdOptions::Bloom(o)) => {
            put_u8(buf, 11);
            put_i32(buf, o.bloom_length);
            for b in o.bit_size.iter() {
                put_i32(buf, *b);
            }
        }
    }
}

fn parse_options(rd: &mut Rd<'_>) -> Option<Option<RdOptions>> {
    match rd.u8()? {
        0 => Some(None),
        1 => Some(Some(RdOptions::Std(StdRdOptions {
            fillfactor: rd.i32()?,
            toast_tuple_target: rd.i32()?,
            autovacuum: AutoVacOpts {
                enabled: rd.boolean()?,
                vacuum_threshold: rd.i32()?,
                vacuum_max_threshold: rd.i32()?,
                vacuum_ins_threshold: rd.i32()?,
                analyze_threshold: rd.i32()?,
                vacuum_cost_limit: rd.i32()?,
                freeze_min_age: rd.i32()?,
                freeze_max_age: rd.i32()?,
                freeze_table_age: rd.i32()?,
                multixact_freeze_min_age: rd.i32()?,
                multixact_freeze_max_age: rd.i32()?,
                multixact_freeze_table_age: rd.i32()?,
                log_min_duration: rd.i32()?,
                vacuum_cost_delay: rd.f64()?,
                vacuum_scale_factor: rd.f64()?,
                vacuum_ins_scale_factor: rd.f64()?,
                analyze_scale_factor: rd.f64()?,
            },
            user_catalog_table: rd.boolean()?,
            parallel_workers: rd.i32()?,
            vacuum_index_cleanup: match rd.i32()? {
                0 => STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
                1 => STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF,
                2 => STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON,
                _ => return None,
            },
            vacuum_truncate: rd.boolean()?,
            vacuum_truncate_set: rd.boolean()?,
            vacuum_max_eager_freeze_failure_rate: rd.f64()?,
        }))),
        2 => Some(Some(RdOptions::View(ViewOptions {
            security_barrier: rd.boolean()?,
            security_invoker: rd.boolean()?,
            check_option: match rd.i32()? {
                0 => VIEW_OPTION_CHECK_OPTION_NOT_SET,
                1 => VIEW_OPTION_CHECK_OPTION_LOCAL,
                2 => VIEW_OPTION_CHECK_OPTION_CASCADED,
                _ => return None,
            },
        }))),
        3 => Some(Some(RdOptions::BTree(BTOptions {
            fillfactor: rd.i32()?,
            vacuum_cleanup_index_scale_factor: rd.f64()?,
            deduplicate_items: rd.boolean()?,
        }))),
        4 => Some(Some(RdOptions::Hash(HashOptions { fillfactor: rd.i32()? }))),
        5 => Some(Some(RdOptions::Gin(GinOptions {
            use_fast_update: rd.boolean()?,
            pending_list_cleanup_size: rd.i32()?,
        }))),
        6 => Some(Some(RdOptions::Gist(GistOptions {
            fillfactor: rd.i32()?,
            buffering_mode: match rd.i32()? {
                0 => GIST_OPTION_BUFFERING_AUTO,
                1 => GIST_OPTION_BUFFERING_ON,
                2 => GIST_OPTION_BUFFERING_OFF,
                _ => return None,
            },
        }))),
        7 => Some(Some(RdOptions::SpGist(SpGistOptions { fillfactor: rd.i32()? }))),
        8 => Some(Some(RdOptions::Brin(BrinOptions {
            pages_per_range: rd.i32()?,
            autosummarize: rd.boolean()?,
        }))),
        9 => {
            let mut o = ::types_rel::PgrcolumnarOptions::default();
            o.codec = match rd.i32()? {
                0 => ::types_rel::PgrcolumnarCodec::Auto,
                1 => ::types_rel::PgrcolumnarCodec::Lz4,
                2 => ::types_rel::PgrcolumnarCodec::Zstd,
                3 => ::types_rel::PgrcolumnarCodec::Plain,
                _ => return None,
            };
            o.zstd_level = rd.i32()?;
            let n = rd.u16()? as usize;
            let ck = core::str::from_utf8(rd.bytes(n)?).ok()?;
            if !o.set_cluster_key(ck) {
                return None;
            }
            let n = rd.u16()? as usize;
            let cc = core::str::from_utf8(rd.bytes(n)?).ok()?;
            if !o.set_codec_cols(cc) {
                return None;
            }
            Some(Some(RdOptions::Pgrcolumnar(o)))
        }
        10 => Some(Some(RdOptions::Hnsw(types_rel::reloptions::HnswOptions {
            m: rd.i32()?,
            ef_construction: rd.i32()?,
        }))),
        11 => {
            let bloom_length = rd.i32()?;
            let mut bit_size = [0i32; 32];
            for b in bit_size.iter_mut() {
                *b = rd.i32()?;
            }
            Some(Some(RdOptions::Bloom(types_rel::reloptions::BloomOptions {
                bloom_length,
                bit_size,
            })))
        }
        _ => None,
    }
}

fn put_opt_str(buf: &mut Buf<'_>, s: &Option<PgString<'_>>) {
    match s {
        None => put_u8(buf, 0),
        Some(s) => {
            put_u8(buf, 1);
            put_u32(buf, s.as_bytes().len() as u32);
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

fn parse_opt_str(rd: &mut Rd<'_>, mcx: Mcx<'static>) -> Option<Option<PgString<'static>>> {
    match rd.u8()? {
        0 => Some(None),
        1 => {
            let len = rd.u32()? as usize;
            let s = core::str::from_utf8(rd.bytes(len)?).ok()?;
            Some(Some(PgString::from_str_in(s, mcx).ok()?))
        }
        _ => None,
    }
}

fn put_oid_vec(buf: &mut Buf<'_>, v: &[Oid]) {
    put_u16(buf, v.len() as u16);
    for &o in v {
        put_u32(buf, o);
    }
}

fn parse_oid_vec(rd: &mut Rd<'_>, mcx: Mcx<'static>) -> Option<PgVec<'static, Oid>> {
    let n = rd.u16()? as usize;
    let mut v = mcx::vec_with_capacity_in(mcx, n).ok()?;
    for _ in 0..n {
        v.push(rd.u32()?);
    }
    Some(v)
}

fn put_i16_vec(buf: &mut Buf<'_>, v: &[i16]) {
    put_u16(buf, v.len() as u16);
    for &o in v {
        put_i16(buf, o);
    }
}

fn parse_i16_vec(rd: &mut Rd<'_>, mcx: Mcx<'static>) -> Option<PgVec<'static, i16>> {
    let n = rd.u16()? as usize;
    let mut v = mcx::vec_with_capacity_in(mcx, n).ok()?;
    for _ in 0..n {
        v.push(rd.i16()?);
    }
    Some(v)
}

fn put_index(buf: &mut Buf<'_>, i: &FormData_pg_index<'_>) {
    put_u32(buf, i.indexrelid);
    put_u32(buf, i.indrelid);
    put_i16(buf, i.indnatts);
    put_i16(buf, i.indnkeyatts);
    put_bool(buf, i.indisunique);
    put_bool(buf, i.indnullsnotdistinct);
    put_bool(buf, i.indisprimary);
    put_bool(buf, i.indisexclusion);
    put_bool(buf, i.indimmediate);
    put_bool(buf, i.indisvalid);
    put_bool(buf, i.indisready);
    put_i16_vec(buf, &i.indkey);
    put_bool(buf, i.has_indpred);
    put_opt_str(buf, &i.indexprs_src);
    put_opt_str(buf, &i.indpred_src);
}

fn parse_index(rd: &mut Rd<'_>, mcx: Mcx<'static>) -> Option<FormData_pg_index<'static>> {
    Some(FormData_pg_index {
        indexrelid: rd.u32()?,
        indrelid: rd.u32()?,
        indnatts: rd.i16()?,
        indnkeyatts: rd.i16()?,
        indisunique: rd.boolean()?,
        indnullsnotdistinct: rd.boolean()?,
        indisprimary: rd.boolean()?,
        indisexclusion: rd.boolean()?,
        indimmediate: rd.boolean()?,
        indisvalid: rd.boolean()?,
        indisready: rd.boolean()?,
        indkey: parse_i16_vec(rd, mcx)?,
        has_indpred: rd.boolean()?,
        indexprs_src: parse_opt_str(rd, mcx)?,
        indpred_src: parse_opt_str(rd, mcx)?,
    })
}

pub(crate) fn encode_entry(buf: &mut Buf<'_>, rel: &RelationData<'static>, nailed: bool) {
    put_bool(buf, nailed);
    put_u32(buf, rel.rd_id);
    put_i32(buf, rel.rd_backend);
    put_bool(buf, rel.rd_islocaltemp);
    put_class(buf, &rel.rd_rel);
    put_bool(buf, rel.rd_hastriggers);
    put_bool(buf, rel.rd_hasrules);
    put_u16(buf, rel.rd_att.natts as u16);
    for a in rel.rd_att.attrs.iter() {
        put_attr(buf, a);
    }
    put_options(buf, &rel.rd_options);
    if rel.rd_rel.relkind == RELKIND_INDEX {
        put_index(buf, rel.rd_index.as_ref().expect("index entry without rd_index"));
        put_oid_vec(buf, &rel.rd_opcintype);
        put_oid_vec(buf, &rel.rd_opfamily);
        put_i16_vec(buf, &rel.rd_indoption);
        put_oid_vec(buf, &rel.rd_indcollation);
        put_oid_vec(buf, &rel.rd_support);
    }
}

fn parse_entry(rd: &mut Rd<'_>, mcx: Mcx<'static>) -> Option<(RelationData<'static>, bool)> {
    let nailed = rd.boolean()?;
    let rd_id = rd.u32()?;
    let rd_backend = rd.i32()?;
    let rd_islocaltemp = rd.boolean()?;
    let form = parse_class(rd)?;
    let rd_hastriggers = rd.boolean()?;
    let rd_hasrules = rd.boolean()?;

    let natts = rd.u16()? as usize;
    if natts == 0 || natts > MAX_ATTS {
        return None;
    }
    let mut attrs: PgVec<'static, FormData_pg_attribute> =
        mcx::vec_with_capacity_in(mcx, natts).ok()?;
    let mut has_not_null = false;
    for _ in 0..natts {
        let a = parse_attr(rd)?;
        has_not_null |= a.attnotnull;
        attrs.push(a);
    }
    let mut td = tupdesc::CreateTupleDesc(mcx, &attrs).ok()?;
    td.tdtypeid = if form.reltype != 0 { form.reltype } else { RECORDOID };
    td.tdtypmod = -1;
    td.tdrefcount = 1;
    if has_not_null {
        td.constr = Some(mcx::box_new_in(
            mcx,
            TupleConstr {
                defval: PgVec::new_in(mcx),
                check: PgVec::new_in(mcx),
                missing: PgVec::new_in(mcx),
                num_defval: 0,
                num_check: 0,
                has_not_null: true,
                has_generated_stored: false,
                has_generated_virtual: false,
            },
        ));
    }

    let rd_options = parse_options(rd)?;

    let (rd_index, opcintype, opfamily, indoption, indcollation, support, supportinfo) =
        if form.relkind == RELKIND_INDEX {
            let idx = parse_index(rd, mcx)?;
            let nkey = idx.indnkeyatts as usize;
            let opcintype = parse_oid_vec(rd, mcx)?;
            let opfamily = parse_oid_vec(rd, mcx)?;
            let indoption = parse_i16_vec(rd, mcx)?;
            let indcollation = parse_oid_vec(rd, mcx)?;
            let support = parse_oid_vec(rd, mcx)?;
            if nkey == 0
                || opcintype.len() != nkey
                || opfamily.len() != nkey
                || indoption.len() != nkey
                || indcollation.len() != nkey
                || support.len() % nkey != 0
            {
                return None;
            }
            // Same slot-0 preload as RelationInitIndexAccessInfo: btree/hash
            // resolve BTORDER_PROC eagerly, other AMs dispatch lazily.
            let amsupport = support.len() / nkey;
            let mut supportinfo: Vec<Option<types_fmgr::FmgrInfo>> = Vec::with_capacity(nkey);
            for i in 0..nkey {
                let proc = if form.relam == BTREE_AM_OID || form.relam == HASH_AM_OID {
                    *support.get(i * amsupport)?
                } else {
                    0
                };
                supportinfo.push(if proc != 0 {
                    Some(fmgr_seams::fmgr_info::call(proc).ok()?)
                } else {
                    None
                });
            }
            (Some(idx), opcintype, opfamily, indoption, indcollation, support, supportinfo)
        } else {
            (
                None,
                PgVec::new_in(mcx),
                PgVec::new_in(mcx),
                PgVec::new_in(mcx),
                PgVec::new_in(mcx),
                PgVec::new_in(mcx),
                Vec::new(),
            )
        };

    let data = RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id,
        rd_backend,
        rd_islocaltemp,
        rd_isvalid: core::cell::Cell::new(true),
        rd_createSubid: core::cell::Cell::new(InvalidSubTransactionId),
        rd_newRelfilelocatorSubid: core::cell::Cell::new(InvalidSubTransactionId),
        rd_firstRelfilelocatorSubid: core::cell::Cell::new(InvalidSubTransactionId),
        rd_droppedSubid: core::cell::Cell::new(InvalidSubTransactionId),
        rd_lockInfo: lmgr::RelationInitLockInfo(rd_id, form.relisshared),
        rd_rel: form,
        rd_att: Rc::new(td),
        rd_index,
        rd_opcintype: opcintype,
        rd_opfamily: opfamily,
        rd_indoption: indoption,
        rd_indcollation: indcollation,
        rd_options,
        pgstat_enabled: core::cell::Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: support,
        rd_supportinfo: core::cell::RefCell::new(supportinfo),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers,
        rd_hasrules,
    };
    Some((data, nailed))
}

// std Vec: droppy Rc payloads can't live in arena collections (rd_supportinfo
// precedent); boot-only scratch.
type ParsedEntries = Vec<(RelationData<'static>, bool)>;

pub(crate) fn parse_init_file(data: &[u8], mcx: Mcx<'static>) -> Option<(ParsedEntries, usize, usize)> {
    let mut rd = Rd { b: data, off: 0 };
    if rd.i32()? != RELCACHE_INIT_FILEMAGIC || rd.u32()? != RELCACHE_INIT_FORMAT {
        return None;
    }
    let mut rels = ParsedEntries::new();
    let mut nailed_rels = 0usize;
    let mut nailed_indexes = 0usize;
    while !rd.at_end() {
        let (data, nailed) = parse_entry(&mut rd, mcx)?;
        // Recompute physical addressing: relmapped rels and CREATE DATABASE
        // copies must not trust the stored relfilenumber.
        build::RelationInitPhysicalAddr(&data).ok()?;
        if nailed {
            if data.rd_rel.relkind == RELKIND_INDEX {
                nailed_indexes += 1;
            } else {
                nailed_rels += 1;
            }
        }
        rels.push((data, nailed));
    }
    Some((rels, nailed_rels, nailed_indexes))
}

fn load_relcache_init_file(shared: bool) -> PgResult<bool> {
    let Some(path) = init_file_path(shared) else {
        return Ok(false);
    };
    // vfs-routed (provider-seam reroute): init files are datadir domain;
    // std::fs would bypass the sim namespace.
    let path_s = path.to_str().expect("datadir paths are UTF-8");
    let Ok(bytes) = fd::read_whole_file(path_s) else {
        return Ok(false);
    };
    // A newer C pg_internal.init beside ours means a C backend served this
    // datadir after we wrote our file; C's DDL unlinks only its own name, so
    // ours may be stale — rebuild. (Under sim, vfs mtimes are all zero and
    // no C backend can enter the sim world, so this never fires there.)
    let mtime_of = |p: &Path| -> Option<(i64, i64)> {
        let mut st = fd::FileInfo::zeroed();
        (fd::pg_stat(p.to_str()?, &mut st) == 0).then_some((st.mtime_sec, st.mtime_nsec))
    };
    if let (Some(ours), Some(theirs)) = (
        mtime_of(&path),
        mtime_of(&path.with_file_name(C_RELCACHE_INIT_FILENAME)),
    ) {
        if theirs > ours {
            return Ok(false);
        }
    }
    let mcx = crate::cache_mcx();
    let Some((rels, nailed_rels, nailed_indexes)) = parse_init_file(&bytes, mcx) else {
        return Ok(false);
    };
    let (exp_rels, exp_indexes) = if shared {
        (NUM_CRITICAL_SHARED_RELS, NUM_CRITICAL_SHARED_INDEXES)
    } else {
        (NUM_CRITICAL_LOCAL_RELS, NUM_CRITICAL_LOCAL_INDEXES)
    };
    if nailed_rels != exp_rels || nailed_indexes != exp_indexes {
        let kind = if shared { " shared" } else { "" };
        elog::elog(
            WARNING,
            format!(
                "found {nailed_rels} nailed{kind} rels and {nailed_indexes} nailed{kind} \
                 indexes in init file, but expected {exp_rels} and {exp_indexes} respectively"
            ),
        )?;
        return Ok(false);
    }
    for (data, nailed) in rels {
        store::insert(Rc::new(data), nailed, false)?;
    }
    with_state(|st| {
        if shared {
            st.critical_shared_relcaches_built = true;
        } else {
            st.critical_relcaches_built = true;
        }
    });
    Ok(true)
}

#[track_caller]
#[cold]
#[inline(never)]
fn could_not_write(e: std::io::Error) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not write init file: {e}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

fn write_relcache_init_file(shared: bool) -> PgResult<()> {
    if with_state(|st| st.invals_received != 0) {
        return Ok(());
    }
    let Some(final_path) = init_file_path(shared) else {
        return Ok(());
    };
    // Snapshot outside the state borrow: RelationIdIsInInitFile crosses seams.
    let entries: Vec<(Rc<RelationData<'static>>, bool)> = with_state(|st| {
        st.id_cache.iter().map(|(_, e)| (Rc::clone(&e.rel), e.nailed)).collect()
    });

    let cx = MemoryContext::new("RelCacheInitFileWrite");
    let mut buf: Buf<'_> = PgVec::new_in(cx.mcx());
    put_i32(&mut buf, RELCACHE_INIT_FILEMAGIC);
    put_u32(&mut buf, RELCACHE_INIT_FORMAT);
    for (rel, nailed) in &entries {
        if rel.rd_rel.relisshared != shared {
            continue;
        }
        // Local file: only rels a relcache inval would invalidate the file for.
        if !shared && !RelationIdIsInInitFile(rel.rd_id) {
            debug_assert!(!*nailed);
            continue;
        }
        encode_entry(&mut buf, rel, *nailed);
    }

    // Temp file + rename: a backend starting concurrently must never see a
    // partially written file.
    let temp_path = final_path.with_file_name(format!(
        "{}.{}",
        RELCACHE_INIT_FILENAME,
        init_small::globals::process_id()
    ));
    // vfs-routed (provider-seam reroute): datadir-domain create/write/rename
    // must ride the vfs; std::fs would bypass the sim namespace.
    let temp_s = temp_path.to_str().expect("datadir paths are UTF-8");
    let _ = fd::pg_unlink(temp_s);
    let fd_ = match fd::OpenTransientFile(temp_s, libc::O_CREAT | libc::O_TRUNC | libc::O_WRONLY) {
        Ok(f) if f >= 0 => f,
        _ => {
            let e = std::io::Error::from_raw_os_error(fd::get_errno());
            elog::elog(
                WARNING,
                format!(
                    "could not create relation-cache initialization file \"{}\": {e}",
                    temp_path.display()
                ),
            )?;
            return Ok(());
        }
    };
    let mut off: usize = 0;
    while off < buf.len() {
        let n = fd::pg_pwrite(fd_, &buf[off..], off as i64);
        if n <= 0 {
            let e = std::io::Error::from_raw_os_error(fd::get_errno());
            fd::CloseTransientFile(fd_);
            return Err(could_not_write(e));
        }
        off += n as usize;
    }
    if fd::CloseTransientFile(fd_) != 0 {
        return Err(could_not_write(std::io::Error::from_raw_os_error(fd::get_errno())));
    }

    // Stale-write race: recheck invals under RelCacheInitLock after draining
    // SI; another backend's committed DDL between our snapshot and here must
    // win (its unlink already happened or its SI reaches us now).
    let lock = lwlock::main_lock(RELCACHE_INIT_LOCK_OFFSET);
    lwlock::LWLockAcquire(lock, lwlock::LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    let result = (|| -> PgResult<()> {
        inval_seams::accept_invalidation_messages::call()?;
        if with_state(|st| st.invals_received == 0) {
            let final_s = final_path.to_str().expect("datadir paths are UTF-8");
            if fd::pg_rename(temp_s, final_s) < 0 {
                let _ = fd::pg_unlink(temp_s);
            }
        } else {
            let _ = fd::pg_unlink(temp_s);
        }
        Ok(())
    })();
    lwlock::LWLockRelease(lock)?;
    result
}

pub fn RelationIdIsInInitFile(relationId: Oid) -> bool {
    if relationId == CAT_PG_SHSECLABEL.relid
        || relationId == TRIGGER_RELID_NAME_INDEX_ID
        || relationId == DATABASE_NAME_INDEX_ID
        || relationId == SHARED_SEC_LABEL_OBJECT_INDEX_ID
    {
        // Init-file members without syscache support (C asserts the same).
        debug_assert!(!syscache_seams::relation_supports_sys_cache::call(relationId));
        return true;
    }
    syscache_seams::relation_supports_sys_cache::call(relationId)
}

fn unlink_initfile(path: &Path, error_level: bool) -> PgResult<()> {
    // vfs-routed (provider-seam reroute).
    let path_s = path.to_str().expect("datadir paths are UTF-8");
    if fd::pg_unlink(path_s) == 0 || fd::get_errno() == libc::ENOENT {
        return Ok(());
    }
    if error_level {
        let e = std::io::Error::from_raw_os_error(fd::get_errno());
        return Err(Box::new(
            PgError::error(format!("could not remove cache file \"{}\": {e}", path.display()))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        ));
    }
    Ok(())
}

fn unlink_both(dir: &Path, error_level: bool) -> PgResult<()> {
    unlink_initfile(&dir.join(RELCACHE_INIT_FILENAME), error_level)?;
    unlink_initfile(&dir.join(C_RELCACHE_INIT_FILENAME), error_level)
}

// Serializes against write_relcache_init_file via RelCacheInitLock: unlink
// under the lock, send SI between Pre and Post, release in Post.
pub fn RelationCacheInitFilePreInvalidate() -> PgResult<()> {
    let lock = lwlock::main_lock(RELCACHE_INIT_LOCK_OFFSET);
    lwlock::LWLockAcquire(lock, lwlock::LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    if let Some(db) = init_small::globals::DatabasePath() {
        unlink_both(Path::new(db), true)?;
    }
    unlink_both(Path::new("global"), true)
}

pub fn RelationCacheInitFilePostInvalidate() -> PgResult<()> {
    lwlock::LWLockRelease(lwlock::main_lock(RELCACHE_INIT_LOCK_OFFSET))
}

// Startup removal: init files may be stale after crash recovery / PITR.
// vfs-routed walks (provider-seam reroute): the stale files live in the
// datadir namespace, which is simulated under sim.
pub fn RelationCacheInitFileRemove() {
    let _ = unlink_both(Path::new("global"), false);
    remove_in_dir(Path::new("base"));
    let _ = (|| -> PgResult<()> {
        let dir = fd::AllocateDir(PG_TBLSPC_DIR)?;
        while let Some(de) = fd::ReadDirExtended(dir, PG_TBLSPC_DIR, DEBUG1)? {
            if de.d_name.bytes().all(|b| b.is_ascii_digit()) {
                remove_in_dir(
                    &Path::new(PG_TBLSPC_DIR).join(&de.d_name).join(TABLESPACE_VERSION_DIRECTORY),
                );
            }
        }
        fd::FreeDir(dir)?;
        Ok(())
    })();
}

fn remove_in_dir(tblspc: &Path) {
    let _ = (|| -> PgResult<()> {
        let dirname = tblspc.to_str().expect("datadir paths are UTF-8");
        let dir = fd::AllocateDir(dirname)?;
        while let Some(de) = fd::ReadDirExtended(dir, dirname, DEBUG1)? {
            if de.d_name.bytes().all(|b| b.is_ascii_digit()) {
                let _ = unlink_both(&tblspc.join(&de.d_name), false);
            }
        }
        fd::FreeDir(dir)?;
        Ok(())
    })();
}
