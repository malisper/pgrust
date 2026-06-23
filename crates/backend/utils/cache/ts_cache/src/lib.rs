//! `utils/cache/ts_cache.c` — tsearch related object caches.
//!
//! Tsearch performance is very sensitive to performance of parsers,
//! dictionaries and mapping, so lookups should be cached as much as possible.
//!
//! Once a backend has created a cache entry for a particular TS object OID,
//! the cache entry will exist for the life of the backend; C returns pointers
//! into the cache on that basis. The owned port returns by-value copies (the
//! config entry's dictionary map copied into the caller's `mcx`); the
//! lifetime guarantee callers relied on becomes ordinary ownership.
//!
//! C's `FmgrInfo` members (`prsstart`, `lexize`, ...) embed resolved function
//! pointers and cannot cross the unported fmgr boundary; the entries keep the
//! function OIDs (which C also stores) and the eager `fmgr_info_cxt`
//! lookup-failure surface is preserved via the fmgr seam's `fmgr_info_check`.
//! Dictionary private data (`dictData`, a `void *` produced by the template's
//! init method) crosses as the opaque pointer `Datum` word the init call
//! returned.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use ::heaptuple::heap_deform_tuple;
use ::scankey::ScanKeyInit;
use genam_seams as genam_seams;
use indexam_seams as indexam_seams;
use table::{table_close, table_open};
use transam_xact_seams as xact_seams;
use namespace_seams as namespace_seams;
use tsearchcmds_seams as tsearchcmds_seams;
use regproc_seams as regproc_seams;
use ruleutils_seams as ruleutils_seams;
use inval_seams as inval_seams;
use lsyscache_seams as lsyscache_seams;
use cache_syscache as syscache;
use error_seams as error_seams;
use fmgr_seams as fmgr_seams;
use init_small_seams as init_small_seams;
use mcx::{vec_with_capacity_in, McxOwned, Mcx, MemoryContext, PgHashMap, PgVec};
use ::cache::SysCacheKey;
use ::types_core::fmgr::F_OIDEQ;
use types_core::{InvalidOid, Oid, OidIsValid};
// The migrated by-value tuple surface uses the canonical
// `types_tuple::...::Datum<'mcx>` enum directly. The bare-word newtype
// `::datum::Datum` (here aliased `ScalarWord`) survives only at the
// audited ABI edges where an *unchanged* cross-crate contract still speaks the
// plain scalar word: the syscache key (`SysCacheKey::Value`), the scankey
// argument (`ScanKeyData::sk_argument`), the syscache-invalidation callback
// `arg` (`SyscacheCallbackFunction`), and the opaque per-template `dictData`
// `void *` word returned by the dictionary-init fmgr seam.
use ::datum::Datum as ScalarWord;
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT, NOTICE};
use types_guc::{GucSource, PGC_S_TEST};
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use ::types_scan::sdir::ForwardScanDirection;
use ::types_storage::lock::AccessShareLock;
use types_tuple::heaptuple::{Datum, FormedTuple};

/// `MAXTOKENTYPE` / `MAXDICTSPERTT` — arbitrary limits on the workspace size
/// used in `lookup_ts_config_cache`. We could avoid hardwiring a limit by
/// making the workspace dynamically enlargeable, but it seems unlikely to be
/// worth the trouble.
const MAXTOKENTYPE: usize = 256;
const MAXDICTSPERTT: usize = 100;

/// `TSConfigMapRelationId` / `TSConfigMapIndexId`
/// (`catalog/pg_ts_config_map.h`).
const TSConfigMapRelationId: Oid = 3603;
const TSConfigMapIndexId: Oid = 3609;

// Attribute numbers of the TS catalogs (`catalog/pg_ts_*.h`).
const Anum_pg_ts_parser_prsstart: i32 = 4;
const Anum_pg_ts_parser_prstoken: i32 = 5;
const Anum_pg_ts_parser_prsend: i32 = 6;
const Anum_pg_ts_parser_prsheadline: i32 = 7;
const Anum_pg_ts_parser_prslextype: i32 = 8;
const Anum_pg_ts_dict_dictname: i32 = 2;
const Anum_pg_ts_dict_dicttemplate: i32 = 5;
const Anum_pg_ts_dict_dictinitoption: i32 = 6;
const Anum_pg_ts_template_tmplinit: i32 = 4;
const Anum_pg_ts_template_tmpllexize: i32 = 5;
const Anum_pg_ts_config_cfgname: i32 = 2;
const Anum_pg_ts_config_cfgnamespace: i32 = 3;
const Anum_pg_ts_config_cfgparser: i32 = 5;
const Anum_pg_ts_config_map_mapcfg: i32 = 1;
const Anum_pg_ts_config_map_maptokentype: i32 = 2;
const Anum_pg_ts_config_map_mapdict: i32 = 4;

/* ---------------------------------------------------------------------------
 * Entry types (`tsearch/ts_cache.h`)
 * ------------------------------------------------------------------------- */

/// `TSParserCacheEntry`. The C `FmgrInfo prsstart/prstoken/prsend/
/// prsheadline` members are represented by their OIDs (`startOid`, ...);
/// callers dispatch by OID through the fmgr seam.
#[derive(Clone, Copy, Debug)]
pub struct TSParserCacheEntry {
    pub prsId: Oid,
    pub isvalid: bool,
    pub startOid: Oid,
    pub tokenOid: Oid,
    pub endOid: Oid,
    pub headlineOid: Oid,
    pub lextypeOid: Oid,
}

/// `TSDictionaryCacheEntry`, as returned to callers. `dict_data` is the
/// opaque `void *` the template's init method returned (`None` when the
/// template has no init method).
#[derive(Clone, Copy, Debug)]
pub struct TSDictionaryCacheEntry {
    pub dictId: Oid,
    pub isvalid: bool,
    pub lexizeOid: Oid,
    pub dict_data: Option<ScalarWord>,
}

/// `ListDictionary` — one token type's dictionary list.
#[derive(Debug)]
pub struct ListDictionary<'mcx> {
    pub dict_ids: PgVec<'mcx, Oid>,
}

/// `TSConfigCacheEntry`, as returned to callers (the map copied into the
/// caller's `mcx`; `map[i]` is empty for token types with no dictionaries,
/// mirroring the C zeroed `ListDictionary`).
#[derive(Debug)]
pub struct TSConfigCacheEntry<'mcx> {
    pub cfgId: Oid,
    pub isvalid: bool,
    pub prsId: Oid,
    /// `lenmap` == `map.len()`.
    pub map: PgVec<'mcx, ListDictionary<'mcx>>,
}

/* ---------------------------------------------------------------------------
 * Per-backend state (the ts_cache.c file-scope statics)
 * ------------------------------------------------------------------------- */

/// Internal dictionary entry: the public fields plus the entry's private
/// memory context (`dictCtx`, a `CacheMemoryContext` child in C; kept for
/// identifier/reset parity — the dictionary's own allocations live behind
/// the fmgr seam until tsearch lands).
struct DictEntry {
    entry: TSDictionaryCacheEntry,
    dict_ctx: MemoryContext,
}

/// Internal config entry: dictionary lists allocated in the cache's context.
struct ConfigEntry<'mcx> {
    cfg_id: Oid,
    isvalid: bool,
    prs_id: Oid,
    /// `(len, dictIds)` per token type, `0..lenmap`.
    map: PgVec<'mcx, PgVec<'mcx, Oid>>,
}

struct TsCacheState<'mcx> {
    mcx: Mcx<'mcx>,
    /// `static HTAB *TSParserCacheHash = NULL` (+ `lastUsedParser`).
    parser_hash: Option<PgHashMap<'mcx, Oid, TSParserCacheEntry>>,
    last_used_parser: Option<Oid>,
    /// `static HTAB *TSDictionaryCacheHash = NULL` (+ `lastUsedDictionary`).
    dict_hash: Option<PgHashMap<'mcx, Oid, DictEntry>>,
    last_used_dictionary: Option<Oid>,
    /// `static HTAB *TSConfigCacheHash = NULL` (+ `lastUsedConfig`).
    config_hash: Option<PgHashMap<'mcx, Oid, ConfigEntry<'mcx>>>,
    last_used_config: Option<Oid>,
    /// GUC `default_text_search_config` (`char *TSCurrentConfig`). C's GUC
    /// machinery owns the string storage and this file holds the variable;
    /// here the crate owns both (see `assign_default_text_search_config`).
    ts_current_config: Option<String>,
    /// `static Oid TSCurrentConfigCache = InvalidOid`.
    ts_current_config_cache: Oid,
}

::mcx::bind!(TsCacheStateTy => TsCacheState<'mcx>);

thread_local! {
    static STATE: RefCell<Option<McxOwned<TsCacheStateTy>>> = const { RefCell::new(None) };
}

/// Run `f` over the backend-local state, creating it on first use. Callers
/// must not re-enter (catalog reads happen outside this borrow so that an
/// invalidation callback fired mid-read can take it).
fn with_state<R>(f: impl for<'mcx> FnOnce(&mut TsCacheState<'mcx>) -> R) -> R {
    STATE.with(|s| {
        let mut slot = s.borrow_mut();
        if slot.is_none() {
            let owned = McxOwned::<TsCacheStateTy>::try_new(
                MemoryContext::new("Tsearch cache"),
                |mcx| {
                    Ok(TsCacheState {
                        mcx,
                        parser_hash: None,
                        last_used_parser: None,
                        dict_hash: None,
                        last_used_dictionary: None,
                        config_hash: None,
                        last_used_config: None,
                        ts_current_config: None,
                        ts_current_config_cache: InvalidOid,
                    })
                },
            )
            .expect("allocating the empty ts_cache state cannot fail");
            *slot = Some(owned);
        }
        slot.as_mut().unwrap().with_mut(f)
    })
}

/// Which cache an invalidation callback targets, encoded in the callback
/// `arg` Datum (C passes the hash table's address there).
const TS_CACHE_PARSER: usize = 1;
const TS_CACHE_DICT: usize = 2;
const TS_CACHE_CONFIG: usize = 3;

/// `InvalidateTSCacheCallBack` — detect when a visible change to a TS catalog
/// entry has been made, by either our own backend or another one.
///
/// In principle we could just flush the specific cache entry that changed,
/// but given that TS configuration changes are probably infrequent, it
/// doesn't seem worth the trouble to determine that; we just flush all the
/// entries of the related hash table. The same function serves all TS caches,
/// selected by `arg`.
fn InvalidateTSCacheCallBack(arg: ScalarWord, _cacheid: i32, _hashvalue: u32) {
    with_state(|st| {
        match arg.as_usize() {
            TS_CACHE_PARSER => {
                if let Some(hash) = st.parser_hash.as_mut() {
                    for entry in hash.values_mut() {
                        entry.isvalid = false;
                    }
                }
            }
            TS_CACHE_DICT => {
                if let Some(hash) = st.dict_hash.as_mut() {
                    for entry in hash.values_mut() {
                        entry.entry.isvalid = false;
                    }
                }
            }
            TS_CACHE_CONFIG => {
                if let Some(hash) = st.config_hash.as_mut() {
                    for entry in hash.values_mut() {
                        entry.isvalid = false;
                    }
                }
                // Also invalidate the current-config cache if it's
                // pg_ts_config.
                st.ts_current_config_cache = InvalidOid;
            }
            // The tag rides a Datum (not type-constrained), so an unknown
            // value is a wiring bug, not an impossible arm: assert in debug
            // builds, no-op in release (the C callback has no such arm).
            _ => debug_assert!(false, "unknown TS cache tag in invalidation arg"),
        }
    });
}

/* ---------------------------------------------------------------------------
 * Small helpers
 * ------------------------------------------------------------------------- */

fn elog_error<T>(message: String) -> PgResult<T> {
    Err(PgError::error(message))
}

fn byval_oid(value: Datum<'_>) -> PgResult<Oid> {
    match &value {
        Datum::ByVal(_) => Ok(value.as_oid()),
        Datum::ByRef(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => elog_error("ts_cache: expected a by-value oid".into()),
    }
}

fn getattr_oid(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<Oid> {
    byval_oid(syscache::SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?)
}

/// `NameStr` of a `name` attribute, as an owned Rust string.
fn getattr_name(
    mcx: Mcx<'_>,
    cache_id: i32,
    tup: &FormedTuple<'_>,
    attnum: i32,
) -> PgResult<String> {
    let value = syscache::SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?;
    match &value {
        Datum::ByRef(b) => {
            let len = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            Ok(String::from_utf8_lossy(&b[..len]).into_owned())
        }
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => elog_error("ts_cache: name attribute is by-value".into()),
    }
}

/* ---------------------------------------------------------------------------
 * lookup_ts_parser_cache
 * ------------------------------------------------------------------------- */

/// `lookup_ts_parser_cache` — fetch parser cache entry.
pub fn lookup_ts_parser_cache(prsId: Oid) -> PgResult<TSParserCacheEntry> {
    let needs_init = with_state(|st| st.parser_hash.is_none());
    if needs_init {
        // First time through: initialize the hash table, flush cache on
        // pg_ts_parser changes. (The C "make sure CacheMemoryContext exists"
        // has no counterpart: the cache context is created with the state.)
        with_state(|st| {
            st.parser_hash = Some(PgHashMap::new_in(st.mcx));
        });
        inval_seams::cache_register_syscache_callback::call(
            syscache::TSPARSEROID,
            InvalidateTSCacheCallBack,
            ScalarWord::from_usize(TS_CACHE_PARSER),
        )?;
    }

    // Check single-entry cache, then the hash table.
    let existing = with_state(|st| {
        let hash = st.parser_hash.as_ref().expect("initialized above");
        if let Some(last) = st.last_used_parser {
            if last == prsId {
                if let Some(e) = hash.get(&prsId) {
                    if e.isvalid {
                        return Some(*e);
                    }
                }
            }
        }
        hash.get(&prsId).copied()
    });

    let entry = match existing {
        Some(e) if e.isvalid => e,
        _ => {
            // If we didn't find one, we want to make one. But first look up
            // the object to be sure the OID is real.
            let (start_oid, token_oid, end_oid, headline_oid, lextype_oid) = {
                let scratch = MemoryContext::new("ts parser cache lookup");
                let mcx = scratch.mcx();
                let tp = syscache::SearchSysCache1(
                    mcx,
                    syscache::TSPARSEROID,
                    SysCacheKey::Value(ScalarWord::from_oid(prsId)),
                )?;
                let Some(tup) = tp else {
                    return elog_error(format!(
                        "cache lookup failed for text search parser {prsId}"
                    ));
                };
                let start_oid =
                    getattr_oid(mcx, syscache::TSPARSEROID, &tup, Anum_pg_ts_parser_prsstart)?;
                let token_oid =
                    getattr_oid(mcx, syscache::TSPARSEROID, &tup, Anum_pg_ts_parser_prstoken)?;
                let end_oid =
                    getattr_oid(mcx, syscache::TSPARSEROID, &tup, Anum_pg_ts_parser_prsend)?;
                let headline_oid =
                    getattr_oid(mcx, syscache::TSPARSEROID, &tup, Anum_pg_ts_parser_prsheadline)?;
                let lextype_oid =
                    getattr_oid(mcx, syscache::TSPARSEROID, &tup, Anum_pg_ts_parser_prslextype)?;

                // Sanity checks
                if !OidIsValid(start_oid) {
                    return elog_error(format!(
                        "text search parser {prsId} has no prsstart method"
                    ));
                }
                if !OidIsValid(token_oid) {
                    return elog_error(format!(
                        "text search parser {prsId} has no prstoken method"
                    ));
                }
                if !OidIsValid(end_oid) {
                    return elog_error(format!("text search parser {prsId} has no prsend method"));
                }

                syscache::ReleaseSysCache(tup);
                (start_oid, token_oid, end_oid, headline_oid, lextype_oid)
            };

            // fmgr_info_cxt(entry->startOid, &entry->prsstart,
            // CacheMemoryContext) etc.: the eager resolution's failure
            // surface, preserved through the fmgr seam.
            fmgr_seams::fmgr_info_check::call(start_oid)?;
            fmgr_seams::fmgr_info_check::call(token_oid)?;
            fmgr_seams::fmgr_info_check::call(end_oid)?;
            if OidIsValid(headline_oid) {
                fmgr_seams::fmgr_info_check::call(headline_oid)?;
            }

            let entry = TSParserCacheEntry {
                prsId,
                isvalid: true,
                startOid: start_oid,
                tokenOid: token_oid,
                endOid: end_oid,
                headlineOid: headline_oid,
                lextypeOid: lextype_oid,
            };

            with_state(|st| -> PgResult<()> {
                let mcx = st.mcx;
                let hash = st.parser_hash.as_mut().expect("initialized above");
                hash.try_reserve(1)
                    .map_err(|_| mcx.oom(core::mem::size_of::<TSParserCacheEntry>()))?;
                hash.insert(prsId, entry);
                Ok(())
            })?;
            entry
        }
    };

    with_state(|st| st.last_used_parser = Some(prsId));
    Ok(entry)
}

/// `getTokenTypes`'s parser-cache read (tsearchcmds.c): the parser's
/// `lextypeOid` (InvalidOid when the parser defines no lextype method).
fn parser_lextype_oid(prs_id: Oid) -> PgResult<Oid> {
    Ok(lookup_ts_parser_cache(prs_id)?.lextypeOid)
}

/* ---------------------------------------------------------------------------
 * lookup_ts_dictionary_cache
 * ------------------------------------------------------------------------- */

/// `lookup_ts_dictionary_cache` — fetch dictionary cache entry.
pub fn lookup_ts_dictionary_cache(dictId: Oid) -> PgResult<TSDictionaryCacheEntry> {
    let needs_init = with_state(|st| st.dict_hash.is_none());
    if needs_init {
        with_state(|st| {
            st.dict_hash = Some(PgHashMap::new_in(st.mcx));
        });
        // Flush cache on pg_ts_dict and pg_ts_template changes.
        inval_seams::cache_register_syscache_callback::call(
            syscache::TSDICTOID,
            InvalidateTSCacheCallBack,
            ScalarWord::from_usize(TS_CACHE_DICT),
        )?;
        inval_seams::cache_register_syscache_callback::call(
            syscache::TSTEMPLATEOID,
            InvalidateTSCacheCallBack,
            ScalarWord::from_usize(TS_CACHE_DICT),
        )?;
    }

    // Check single-entry cache, then the hash table.
    let existing = with_state(|st| {
        let hash = st.dict_hash.as_ref().expect("initialized above");
        if let Some(last) = st.last_used_dictionary {
            if last == dictId {
                if let Some(e) = hash.get(&dictId) {
                    if e.entry.isvalid {
                        return Some((e.entry, true));
                    }
                }
            }
        }
        hash.get(&dictId).map(|e| (e.entry, e.entry.isvalid))
    });

    let entry = match existing {
        Some((e, true)) => e,
        other => {
            let had_entry = other.is_some();

            // If we didn't find one, we want to make one. But first look up
            // the object to be sure the OID is real.
            let (dict_ctx, tmpllexize, dict_data) = {
            let scratch = MemoryContext::new("ts dictionary cache lookup");
            let mcx = scratch.mcx();
            let tpdict = syscache::SearchSysCache1(
                mcx,
                syscache::TSDICTOID,
                SysCacheKey::Value(ScalarWord::from_oid(dictId)),
            )?;
            let Some(tpdict) = tpdict else {
                return elog_error(format!(
                    "cache lookup failed for text search dictionary {dictId}"
                ));
            };
            let dicttemplate =
                getattr_oid(mcx, syscache::TSDICTOID, &tpdict, Anum_pg_ts_dict_dicttemplate)?;
            let dictname = getattr_name(mcx, syscache::TSDICTOID, &tpdict, Anum_pg_ts_dict_dictname)?;

            // Sanity checks
            if !OidIsValid(dicttemplate) {
                return elog_error(format!("text search dictionary {dictId} has no template"));
            }

            // Retrieve dictionary's template
            let tptmpl = syscache::SearchSysCache1(
                mcx,
                syscache::TSTEMPLATEOID,
                SysCacheKey::Value(ScalarWord::from_oid(dicttemplate)),
            )?;
            let Some(tptmpl) = tptmpl else {
                return elog_error(format!(
                    "cache lookup failed for text search template {dicttemplate}"
                ));
            };
            let tmplinit =
                getattr_oid(mcx, syscache::TSTEMPLATEOID, &tptmpl, Anum_pg_ts_template_tmplinit)?;
            let tmpllexize =
                getattr_oid(mcx, syscache::TSTEMPLATEOID, &tptmpl, Anum_pg_ts_template_tmpllexize)?;

            // Sanity checks (the C message prints template->tmpllexize, kept
            // verbatim).
            if !OidIsValid(tmpllexize) {
                return elog_error(format!(
                    "text search template {tmpllexize} has no lexize method"
                ));
            }

            // dictCtx: create the private memory context the first time
            // through, else clear the existing entry's private context
            // (resetting its identifier first so it can't dangle).
            let dict_ctx = if had_entry {
                let ctx = with_state(|st| {
                    st.dict_hash
                        .as_mut()
                        .expect("initialized above")
                        .remove(&dictId)
                        .map(|e| e.dict_ctx)
                });
                match ctx {
                    Some(mut ctx) => {
                        ctx.set_ident(None);
                        ctx.reset();
                        ctx.set_ident(Some(&dictname));
                        ctx
                    }
                    None => {
                        // The entry vanished under an invalidation between
                        // our probe and here; build a fresh context.
                        let ctx = MemoryContext::new("TS dictionary");
                        ctx.set_ident(Some(&dictname));
                        ctx
                    }
                }
            } else {
                // saveCtx = AllocSetContextCreate(CacheMemoryContext, "TS
                // dictionary", ALLOCSET_SMALL_SIZES) +
                // MemoryContextCopyAndSetIdentifier(saveCtx, dictname).
                let ctx = MemoryContext::new("TS dictionary");
                ctx.set_ident(Some(&dictname));
                ctx
            };

            let mut dict_data: Option<ScalarWord> = None;
            if OidIsValid(tmplinit) {
                // Init method runs in dictionary's private memory context in
                // C (MemoryContextSwitchTo(entry->dictCtx)); the ambient
                // context cannot cross the seams, so the owner-side
                // allocation placement diverges until fmgr/tsearch land.
                let (opt, isnull) = syscache::SysCacheGetAttr(
                    mcx,
                    syscache::TSDICTOID,
                    &tpdict,
                    Anum_pg_ts_dict_dictinitoption,
                )?;
                let dictoptions = if isnull {
                    PgVec::new_in(mcx) // NIL
                } else {
                    // deserialize_deflist(opt): the owner performs the C
                    // TextDatumGetCString detoast + conversion; the verbatim
                    // varlena bytes cross the seam.
                    let bytes = match &opt {
                        Datum::ByRef(b) => &b[..],
                        Datum::ByVal(_)
                        | Datum::Cstring(_)
                        | Datum::Composite(_)
                        | Datum::Expanded(_)
                        | Datum::Internal(_) => {
                            return elog_error("dictinitoption is not by-reference".into())
                        }
                    };
                    tsearchcmds_seams::deserialize_deflist::call(mcx, bytes)?
                };
                dict_data = Some(fmgr_seams::oid_function_call_1_deflist::call(
                    tmplinit,
                    &dictoptions,
                )?);
            }

            syscache::ReleaseSysCache(tptmpl);
            syscache::ReleaseSysCache(tpdict);
            (dict_ctx, tmpllexize, dict_data)
            };

            // fmgr_info_cxt(entry->lexizeOid, &entry->lexize, entry->dictCtx)
            fmgr_seams::fmgr_info_check::call(tmpllexize)?;

            let entry = TSDictionaryCacheEntry {
                dictId,
                isvalid: true,
                lexizeOid: tmpllexize,
                dict_data,
            };
            with_state(|st| -> PgResult<()> {
                let mcx = st.mcx;
                let hash = st.dict_hash.as_mut().expect("initialized above");
                hash.try_reserve(1)
                    .map_err(|_| mcx.oom(core::mem::size_of::<TSDictionaryCacheEntry>()))?;
                hash.insert(dictId, DictEntry { entry, dict_ctx });
                Ok(())
            })?;
            entry
        }
    };

    with_state(|st| st.last_used_dictionary = Some(dictId));
    Ok(entry)
}

/// The catalog facts a `lexize` dispatcher needs for a dictionary OID: which
/// template (and thus which `*_init` / `*_lexize` method pair) it uses, its
/// lexize method OID (for `get_func_name`-keyed dispatch), and the
/// `deserialize_deflist`-decoded init options.
pub struct TSDictTemplateInfo<'mcx> {
    /// `pg_ts_dict.dicttemplate`.
    pub template_oid: Oid,
    /// `pg_ts_template.tmpllexize` — the lexize method's `pg_proc` OID. Builtin
    /// templates have fixed OIDs; the snowball template's is assigned at initdb,
    /// so callers key on `get_func_name(lexize_oid)` rather than the raw OID.
    pub lexize_oid: Oid,
    /// `deserialize_deflist(pg_ts_dict.dictinitoption)` — the `(defname, arg)`
    /// option list the template's `*_init` method consumes.
    pub options: PgVec<'mcx, ::cache::deflist::DefElemString<'mcx>>,
}

/// Read the template OID + lexize-method OID + decoded init options for a
/// dictionary OID, without going through (or populating) the dictionary cache.
///
/// This is the catalog-read half of C's `lookup_ts_dictionary_cache` (the
/// `pg_ts_dict` -> `pg_ts_template` join), exposed so the owned-model `lexize`
/// dispatcher can rebuild the dictionary object from its options and run the
/// ported `*_lexize` body (the C `dict_data` `void *` round-trip is not
/// reachable by OID in the owned model).
pub fn lookup_ts_dict_template_info<'mcx>(
    mcx: Mcx<'mcx>,
    dictId: Oid,
) -> PgResult<TSDictTemplateInfo<'mcx>> {
    let tpdict = syscache::SearchSysCache1(
        mcx,
        syscache::TSDICTOID,
        SysCacheKey::Value(ScalarWord::from_oid(dictId)),
    )?;
    let Some(tpdict) = tpdict else {
        return elog_error(format!("cache lookup failed for text search dictionary {dictId}"));
    };

    let template_oid = getattr_oid(mcx, syscache::TSDICTOID, &tpdict, Anum_pg_ts_dict_dicttemplate)?;
    if !OidIsValid(template_oid) {
        return elog_error(format!("text search dictionary {dictId} has no template"));
    }

    // Decode the init options (deserialize_deflist of dictinitoption).
    let (opt, isnull) = syscache::SysCacheGetAttr(
        mcx,
        syscache::TSDICTOID,
        &tpdict,
        Anum_pg_ts_dict_dictinitoption,
    )?;
    let options = if isnull {
        PgVec::new_in(mcx)
    } else {
        let bytes = match &opt {
            Datum::ByRef(b) => &b[..],
            Datum::ByVal(_)
            | Datum::Cstring(_)
            | Datum::Composite(_)
            | Datum::Expanded(_)
            | Datum::Internal(_) => {
                return elog_error("dictinitoption is not by-reference".into())
            }
        };
        tsearchcmds_seams::deserialize_deflist::call(mcx, bytes)?
    };

    // Retrieve the dictionary's template to read tmpllexize.
    let tptmpl = syscache::SearchSysCache1(
        mcx,
        syscache::TSTEMPLATEOID,
        SysCacheKey::Value(ScalarWord::from_oid(template_oid)),
    )?;
    let Some(tptmpl) = tptmpl else {
        return elog_error(format!(
            "cache lookup failed for text search template {template_oid}"
        ));
    };
    let lexize_oid =
        getattr_oid(mcx, syscache::TSTEMPLATEOID, &tptmpl, Anum_pg_ts_template_tmpllexize)?;
    if !OidIsValid(lexize_oid) {
        return elog_error(format!(
            "text search template {template_oid} has no lexize method"
        ));
    }

    syscache::ReleaseSysCache(tptmpl);
    syscache::ReleaseSysCache(tpdict);

    Ok(TSDictTemplateInfo {
        template_oid,
        lexize_oid,
        options,
    })
}

/* ---------------------------------------------------------------------------
 * lookup_ts_config_cache
 * ------------------------------------------------------------------------- */

/// `init_ts_config_cache` — initialize config cache and prepare callbacks.
/// Split out of `lookup_ts_config_cache` because we need to activate the
/// callback before caching `TSCurrentConfigCache`, too.
fn init_ts_config_cache() -> PgResult<()> {
    with_state(|st| {
        st.config_hash = Some(PgHashMap::new_in(st.mcx));
    });
    // Flush cache on pg_ts_config and pg_ts_config_map changes.
    inval_seams::cache_register_syscache_callback::call(
        syscache::TSCONFIGOID,
        InvalidateTSCacheCallBack,
        ScalarWord::from_usize(TS_CACHE_CONFIG),
    )?;
    inval_seams::cache_register_syscache_callback::call(
        syscache::TSCONFIGMAP,
        InvalidateTSCacheCallBack,
        ScalarWord::from_usize(TS_CACHE_CONFIG),
    )?;
    Ok(())
}

/// `lookup_ts_config_cache` — fetch configuration cache entry, the
/// dictionary map copied into `mcx`.
pub fn lookup_ts_config_cache<'mcx>(
    mcx: Mcx<'mcx>,
    cfgId: Oid,
) -> PgResult<TSConfigCacheEntry<'mcx>> {
    let needs_init = with_state(|st| st.config_hash.is_none());
    if needs_init {
        // First time through: initialize the hash table.
        init_ts_config_cache()?;
    }

    // Check single-entry cache / existing entry. (The C fast path and the
    // regular lookup both end in the same copy-out here.)
    let valid = with_state(|st| {
        st.config_hash
            .as_ref()
            .expect("initialized above")
            .get(&cfgId)
            .is_some_and(|e| e.isvalid)
    });

    if !valid {
        // If we didn't find one, we want to make one. But first look up the
        // object to be sure the OID is real.
        let (cfgparser, maplists, mapdicts, maxtokentype) = {
        let scratch = MemoryContext::new("ts config cache lookup");
        let smcx = scratch.mcx();
        let tp = syscache::SearchSysCache1(
            smcx,
            syscache::TSCONFIGOID,
            SysCacheKey::Value(ScalarWord::from_oid(cfgId)),
        )?;
        let Some(tup) = tp else {
            return elog_error(format!(
                "cache lookup failed for text search configuration {cfgId}"
            ));
        };
        let cfgparser = getattr_oid(smcx, syscache::TSCONFIGOID, &tup, Anum_pg_ts_config_cfgparser)?;

        // Sanity checks
        if !OidIsValid(cfgparser) {
            return elog_error(format!("text search configuration {cfgId} has no parser"));
        }
        syscache::ReleaseSysCache(tup);

        // Scan pg_ts_config_map to gather dictionary list for each token
        // type.
        //
        // Because the index is on (mapcfg, maptokentype, mapseqno), we will
        // see the entries in maptokentype order, and in mapseqno order for
        // each token type, even though we didn't explicitly ask for that.
        let mut mapskey = [ScanKeyData::empty()];
        ScanKeyInit(
            &mut mapskey[0],
            Anum_pg_ts_config_map_mapcfg as i16,
            BTEqualStrategyNumber,
            F_OIDEQ,
            // `ScanKeyData.sk_argument` is the canonical unified `Datum<'mcx>`
            // (the Datum-unification keystone flipped this edge).
            Datum::from_oid(cfgId),
        )?;
        let maprel = table_open(smcx, TSConfigMapRelationId, AccessShareLock)?;
        let mapidx =
            indexam_seams::index_open::call(smcx, TSConfigMapIndexId, AccessShareLock)?;
        let mut mapscan =
            genam_seams::systable_beginscan_ordered::call(&maprel, &mapidx, None, &mapskey)?;

        // maplists[MAXTOKENTYPE + 1] (zeroed), mapdicts[MAXDICTSPERTT].
        let mut maplists: Vec<Vec<Oid>> = (0..=MAXTOKENTYPE).map(|_| Vec::new()).collect();
        let mut mapdicts: Vec<Oid> = Vec::with_capacity(MAXDICTSPERTT);
        let mut maxtokentype: usize = 0;

        while let Some(maptup) =
            genam_seams::systable_getnext_ordered::call(smcx, mapscan.desc_mut(), ForwardScanDirection)?
        {
            let row = heap_deform_tuple(smcx, &maptup.tuple, &maprel.rd_att, &maptup.data)?;
            let toktype_d = &row[(Anum_pg_ts_config_map_maptokentype - 1) as usize].0;
            let toktype = match toktype_d {
                Datum::ByVal(_) => toktype_d.as_i32(),
                Datum::ByRef(_)
                | Datum::Cstring(_)
                | Datum::Composite(_)
                | Datum::Expanded(_)
                | Datum::Internal(_) => {
                    return elog_error("maptokentype is not by-value".into())
                }
            };
            let mapdict_d = &row[(Anum_pg_ts_config_map_mapdict - 1) as usize].0;
            let mapdict = match mapdict_d {
                Datum::ByVal(_) => mapdict_d.as_oid(),
                Datum::ByRef(_)
                | Datum::Cstring(_)
                | Datum::Composite(_)
                | Datum::Expanded(_)
                | Datum::Internal(_) => return elog_error("mapdict is not by-value".into()),
            };

            if toktype <= 0 || toktype as usize > MAXTOKENTYPE {
                return elog_error(format!("maptokentype value {toktype} is out of range"));
            }
            let toktype = toktype as usize;
            if toktype < maxtokentype {
                return elog_error("maptokentype entries are out of order".into());
            }
            if toktype > maxtokentype {
                // starting a new token type, but first save the prior data
                if !mapdicts.is_empty() {
                    maplists[maxtokentype] = std::mem::take(&mut mapdicts);
                }
                maxtokentype = toktype;
                mapdicts = Vec::with_capacity(MAXDICTSPERTT);
                mapdicts.push(mapdict);
            } else {
                // continuing data for current token type
                if mapdicts.len() >= MAXDICTSPERTT {
                    return elog_error(
                        "too many pg_ts_config_map entries for one token type".into(),
                    );
                }
                mapdicts.push(mapdict);
            }
        }

        mapscan.end()?;
        mapidx.close(AccessShareLock)?;
        table_close(maprel, AccessShareLock)?;
        (cfgparser, maplists, mapdicts, maxtokentype)
        };
        let (mut maplists, mut mapdicts) = (maplists, mapdicts);

        // Save the last token type's dictionaries and the overall map (an
        // empty map when no entries were found, matching the C MemSet base).
        let lenmap = if mapdicts.is_empty() && maxtokentype == 0 {
            0
        } else {
            if !mapdicts.is_empty() {
                maplists[maxtokentype] = std::mem::take(&mut mapdicts);
            }
            maxtokentype + 1
        };

        with_state(|st| -> PgResult<()> {
            let cache_mcx = st.mcx;
            // Copy the workspace into the cache's context.
            let mut map = vec_with_capacity_in(cache_mcx, lenmap)?;
            for dicts in maplists.iter().take(lenmap) {
                let mut ids = vec_with_capacity_in(cache_mcx, dicts.len())?;
                ids.extend_from_slice(dicts);
                map.push(ids);
            }
            let hash = st.config_hash.as_mut().expect("initialized above");
            hash.try_reserve(1)
                .map_err(|_| cache_mcx.oom(core::mem::size_of::<ConfigEntry<'_>>()))?;
            hash.insert(
                cfgId,
                ConfigEntry { cfg_id: cfgId, isvalid: true, prs_id: cfgparser, map },
            );
            Ok(())
        })?;
    }

    with_state(|st| {
        st.last_used_config = Some(cfgId);
        let entry = st
            .config_hash
            .as_ref()
            .expect("initialized above")
            .get(&cfgId)
            .expect("entry inserted above");
        // Copy out into the caller's mcx.
        let mut map = vec_with_capacity_in(mcx, entry.map.len())?;
        for ids in &entry.map {
            let mut out = vec_with_capacity_in(mcx, ids.len())?;
            out.extend_from_slice(ids);
            map.push(ListDictionary { dict_ids: out });
        }
        Ok(TSConfigCacheEntry {
            cfgId: entry.cfg_id,
            isvalid: entry.isvalid,
            prsId: entry.prs_id,
            map,
        })
    })
}

/* ---------------------------------------------------------------------------
 * GUC variable "default_text_search_config"
 * ------------------------------------------------------------------------- */

/// `getTSCurrentConfig` — the OID of the current default text search config,
/// caching the lookup.
pub fn getTSCurrentConfig(emitError: bool) -> PgResult<Oid> {
    // if we have a cached value, return it
    let (cached, config) =
        with_state(|st| (st.ts_current_config_cache, st.ts_current_config.clone()));
    if OidIsValid(cached) {
        return Ok(cached);
    }

    // fail if GUC hasn't been set up yet
    let Some(config) = config.filter(|c| !c.is_empty()) else {
        if emitError {
            return elog_error("text search configuration isn't set".into());
        }
        return Ok(InvalidOid);
    };

    let needs_init = with_state(|st| st.config_hash.is_none());
    if needs_init {
        // First time through: initialize the tsconfig inval callback.
        init_ts_config_cache()?;
    }

    // Look up the config.
    let resolved = {
        let scratch = MemoryContext::new("getTSCurrentConfig");
        let mcx = scratch.mcx();
        if emitError {
            let namelist =
                regproc_seams::string_to_qualified_name_list::call(mcx, &config, false)?
                    .expect("hard-error parse returned no list");
            let parts: Vec<&str> = namelist.iter().map(|s| s.as_str()).collect();
            namespace_seams::get_ts_config_oid::call(&parts, false)?
        } else {
            match regproc_seams::string_to_qualified_name_list::call(mcx, &config, true)? {
                Some(namelist) if !namelist.is_empty() => {
                    let parts: Vec<&str> = namelist.iter().map(|s| s.as_str()).collect();
                    namespace_seams::get_ts_config_oid::call(&parts, true)?
                }
                _ => InvalidOid, // bad name list syntax
            }
        }
    };

    with_state(|st| st.ts_current_config_cache = resolved);
    Ok(resolved)
}

/// `check_default_text_search_config` — GUC check_hook for
/// `default_text_search_config`. On success may rewrite `*newval` to the
/// fully qualified name, so later `search_path` changes don't affect it.
/// `Ok(false)` means "reject the value" (the C `return false`).
///
/// `my_database_id` is C's `MyDatabaseId` (globals.c), passed explicitly —
/// no ambient-global seams; the GUC machinery reads it off its own state
/// when it lands.
pub fn check_default_text_search_config(
    newval: &mut String,
    my_database_id: Oid,
    source: GucSource,
) -> PgResult<bool> {
    // If we aren't inside a transaction, or connected to a database, we
    // cannot do the catalog accesses necessary to verify the config name.
    // Must accept it on faith.
    if xact_seams::is_transaction_state::call() && my_database_id != InvalidOid {
        let scratch = MemoryContext::new("check_default_text_search_config");
        let mcx = scratch.mcx();

        let cfg_id = match regproc_seams::string_to_qualified_name_list::call(mcx, newval, true)? {
            Some(namelist) if !namelist.is_empty() => {
                let parts: Vec<&str> = namelist.iter().map(|s| s.as_str()).collect();
                namespace_seams::get_ts_config_oid::call(&parts, true)?
            }
            _ => InvalidOid, // bad name list syntax
        };

        // When source == PGC_S_TEST, don't throw a hard error for a
        // nonexistent configuration, only a NOTICE. See comments in guc.h.
        if !OidIsValid(cfg_id) {
            if source == PGC_S_TEST {
                error_seams::ereport::call(
                    PgError::new(
                        NOTICE,
                        format!("text search configuration \"{newval}\" does not exist"),
                    )
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
                )?;
                return Ok(true);
            }
            return Ok(false);
        }

        // Modify the actually stored value to be fully qualified, to ensure
        // later changes of search_path don't affect it.
        let tuple = syscache::SearchSysCache1(
            mcx,
            syscache::TSCONFIGOID,
            SysCacheKey::Value(ScalarWord::from_oid(cfg_id)),
        )?;
        let Some(tup) = tuple else {
            return elog_error(format!(
                "cache lookup failed for text search configuration {cfg_id}"
            ));
        };
        let cfgnamespace =
            getattr_oid(mcx, syscache::TSCONFIGOID, &tup, Anum_pg_ts_config_cfgnamespace)?;
        let cfgname = getattr_name(mcx, syscache::TSCONFIGOID, &tup, Anum_pg_ts_config_cfgname)?;

        let nspname = lsyscache_seams::get_namespace_name::call(mcx, cfgnamespace)?;
        let buf = ruleutils_seams::quote_qualified_identifier::call(
            mcx,
            nspname.as_ref().map(|s| s.as_str()),
            &cfgname,
        )?;

        syscache::ReleaseSysCache(tup);

        // GUC wants it guc_malloc'd not palloc'd: guc_free(*newval); *newval
        // = guc_strdup(LOG, buf). The owned `String` replacement cannot fail
        // the way guc_strdup(LOG, ...) can, so the `return false` OOM path
        // has no counterpart.
        *newval = buf.as_str().to_owned();
    }

    Ok(true)
}

/// `assign_default_text_search_config` — GUC assign_hook for
/// `default_text_search_config`: just reset the cache to force a lookup on
/// first use.
///
/// In C the GUC machinery owns the `TSCurrentConfig` string storage and
/// performs the store itself after calling the hook; this crate owns the
/// variable, so the store is folded in here.
pub fn assign_default_text_search_config(newval: Option<&str>) {
    with_state(|st| {
        st.ts_current_config = newval.map(|s| s.to_owned());
        st.ts_current_config_cache = InvalidOid;
    });
}

/// Install this crate's GUC storage variable and hooks into the GUC tables'
/// typed slots (it declares no inward seams of its own).
pub fn init_seams() {
    use guc_tables::{hooks, vars, GucHookExtra, GucVarAccessors};

    fn check_hook(
        newval: &mut Option<String>,
        _extra: &mut Option<GucHookExtra>,
        source: GucSource,
    ) -> PgResult<bool> {
        // default_text_search_config boots to "pg_catalog.simple" (never
        // NULL), so a NULL candidate cannot reach this hook.
        let Some(value) = newval.as_mut() else {
            return Ok(true);
        };
        check_default_text_search_config(value, init_small_seams::my_database_id::call(), source)
    }

    hooks::check_default_text_search_config.install(check_hook);
    hooks::assign_default_text_search_config
        .install(|newval, _extra| assign_default_text_search_config(newval));
    vars::TSCurrentConfig.install(GucVarAccessors {
        get: || with_state(|st| st.ts_current_config.clone()),
        set: |v| with_state(|st| st.ts_current_config = v),
    });

    // getTokenTypes's parser-cache `lextypeOid` read.
    tsearchcmds_seams::parser_lextype_oid::set(parser_lextype_oid);

    // `tsvector_update_trigger` (no-config-column variant) config-name resolution:
    // `cfgId = get_ts_config_oid(stringToQualifiedNameList(tgargs[1]), false)`.
    tsvector_ext_seams::lookup_ts_config::set(lookup_ts_config_impl);
}

/// `get_ts_config_oid(stringToQualifiedNameList(name), false)` (the
/// `tsvector_update_trigger` no-config-column leg). `name` is the raw,
/// server-encoded trigger-arg bytes (no NUL terminator); the schema-qualification
/// requirement is enforced by the caller before this point, so the namelist is
/// always multi-element here.
fn lookup_ts_config_impl(name: &[u8]) -> PgResult<Oid> {
    let scratch = MemoryContext::new("tsvector_update_trigger lookup_ts_config");
    let mcx = scratch.mcx();
    let s = core::str::from_utf8(name)
        .map_err(|_| PgError::error("invalid UTF-8 in text search configuration name"))?;
    let namelist = regproc_seams::string_to_qualified_name_list::call(mcx, s, false)?
        .expect("hard-error parse returned no list");
    let parts: Vec<&str> = namelist.iter().map(|p| p.as_str()).collect();
    namespace_seams::get_ts_config_oid::call(&parts, false)
}
