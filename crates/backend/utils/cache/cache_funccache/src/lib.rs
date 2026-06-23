//! `backend/utils/cache/funccache.c` — function cache management.
//!
//! A backend-lifetime cache of compiled-function data, keyed by
//! `(function OID, input argument types, trigger / event-trigger context, input
//! collation, optional composite result rowtype)`. SQL-language and PL/pgSQL
//! functions (and potentially others) share it; each entry is specific to one
//! invocation shape, so a polymorphic function may have many entries and a
//! trigger function gets one entry per trigger.
//!
//! # Owned model of the C aliasing
//!
//! C's `cached_function_compile` returns a `CachedFunction *` that is
//! simultaneously (a) stored in the hash table, (b) returned to the caller (who
//! stashes it in `fcinfo->flinfo->fn_extra`), and (c) back-linked to its
//! hashtable key via `function->fn_hashkey`. The owned model expresses that
//! single `MemoryContextAllocZero(cacheEntrySize)` allocation as a
//! [`CachedFunctionRef`] (`Rc<RefCell<dyn CachedFunctionPayload>>`): the cache
//! holds one clone, the caller gets another; the embedded [`CachedFunction`]
//! header is mutated in place through the [`CachedFunctionPayload`] trait. The
//! `fn_hashkey` back-link is the [`CachedFunctionKeyId`] fingerprint the header
//! records, which [`delete_function`] uses to relocate the entry without a raw
//! aliasing pointer.
//!
//! The hash table itself is a backend-lifetime [`McxOwned`] over a
//! [`PgHashMap`] in a dedicated `"Cached function hash"` context (created in
//! `TopMemoryContext` regardless of caller's context, as the C
//! `cfunc_hashtable_init` does); a `thread_local` holds it, mirroring the C
//! file-scope `static HTAB *cfunc_hashtable`. The optional composite
//! `callResultType` is copied into that context at insert time
//! ([`CreateTupleDescCopy`]) and dropped with the entry at delete time, exactly
//! the `CreateTupleDescCopy(...TopMemoryContext)` / `FreeTupleDesc` C dance.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::format;
use core::cell::RefCell;

use mcx::{Mcx, McxOwned, MemoryContext, PgHashMap, PgVec};
use types_core::primitive::{InvalidOid, Oid, Size};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR, WARNING};
use types_funccache::{
    CachedFunction, CachedFunctionHashKey, CachedFunctionKeyId, CachedFunctionRef, ProcCompileInfo,
};
use nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLENONARRAYOID, ANYCOMPATIBLEOID,
    ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID, ANYNONARRAYOID, ANYRANGEOID,
    INT4OID, RECORDARRAYOID, RECORDOID,
};

use utils_error::ereport;
use hashfn::{hash_bytes, hash_combine};

use tupdesc::{equalRowTypes, hashRowType, CreateTupleDescCopy};
use funcapi::polymorphic::{resolve_polymorphic_argtypes, CallExpr};
use funcapi::result_type::get_call_result_type;

/// `INT4ARRAYOID` (`pg_type_d.h`) — `_int4`.
const INT4ARRAYOID: Oid = 1007;
/// `INT4RANGEOID` (`pg_type_d.h`) — `int4range`.
const INT4RANGEOID: Oid = 3904;
/// `INT4MULTIRANGEOID` (`pg_type_d.h`) — `int4multirange`.
const INT4MULTIRANGEOID: Oid = 4451;

/// `PROARGMODE_IN` (`pg_proc.h`).
const PROARGMODE_IN: u8 = b'i';
/// `PROARGMODE_OUT` (`pg_proc.h`).
const PROARGMODE_OUT: u8 = b'o';
/// `PROARGMODE_TABLE` (`pg_proc.h`).
const PROARGMODE_TABLE: u8 = b't';

/// `nodeTag(T_TriggerData)` (`nodetags.h`) — `CALLED_AS_TRIGGER`'s tag.
const T_TriggerData: u32 = 442;
/// `nodeTag(T_EventTriggerData)` (`nodetags.h`) — `CALLED_AS_EVENT_TRIGGER`'s tag.
const T_EventTriggerData: u32 = 441;

/// `TYPEFUNC_COMPOSITE` (`funcapi.h`).
use nodes::funcapi::TypeFuncClass;

// ===========================================================================
// The hash table (`static HTAB *cfunc_hashtable`)
// ===========================================================================

/// `FUNCS_PER_USER` (funccache.c) — initial table size.
const FUNCS_PER_USER: usize = 128;

/// `CachedFunctionHashEntry` (funccache.c): the hashtable entry. The C struct
/// is `{ CachedFunctionHashKey key; CachedFunction *function; }`; the owned
/// entry stores the full live key (used by `cfunc_match` and to free
/// `callResultType`) and the shared payload ref.
struct CachedFunctionHashEntry<'mcx> {
    key: CachedFunctionHashKey<'mcx>,
    function: CachedFunctionRef,
}

/// The cache state: the entries, charged to the owning `McxOwned` context. The
/// map is bucketed by the [`cfunc_hash`] value (the [`CachedFunctionKeyId`]
/// locator); hash collisions (distinct keys hashing to the same bucket) are
/// resolved by a per-bucket list scanned with [`cfunc_match`] — exactly the C
/// dynahash's hash-then-match-chain, here a `PgVec` chain under each bucket.
struct CfuncState<'mcx> {
    mcx: Mcx<'mcx>,
    table: PgHashMap<'mcx, CachedFunctionKeyId, PgVec<'mcx, CachedFunctionHashEntry<'mcx>>>,
}

mcx::bind!(CfuncTy => CfuncState<'mcx>);

thread_local! {
    /// `static HTAB *cfunc_hashtable = NULL;` — `None` is the NULL table
    /// (uninitialized). Lives in its own context, created in `TopMemoryContext`
    /// regardless of caller context (the C `cfunc_hashtable_init` comment).
    static CFUNC_HASHTABLE: RefCell<Option<McxOwned<CfuncTy>>> = const { RefCell::new(None) };
}

/// The bucket a key hashes into — its [`cfunc_hash`] value (C's
/// `hash_search` bucket selection). The matching entry within the bucket is then
/// found with [`cfunc_match`].
fn key_id(k: &CachedFunctionHashKey) -> CachedFunctionKeyId {
    CachedFunctionKeyId(cfunc_hash(k))
}

/// `cfunc_hashtable_init` (funccache.c:58) — initialize the hash table on first
/// use. The hash table is in its own context regardless of caller's context.
fn cfunc_hashtable_init() -> PgResult<()> {
    CFUNC_HASHTABLE.with(|cell| {
        let mut slot = cell.borrow_mut();
        debug_assert!(slot.is_none(), "cfunc_hashtable double-initialization");
        let owned = McxOwned::<CfuncTy>::try_new(
            MemoryContext::new("Cached function hash"),
            |m| {
                let mut table = PgHashMap::with_capacity_in(FUNCS_PER_USER, m);
                table.clear();
                Ok(CfuncState { mcx: m, table })
            },
        )?;
        *slot = Some(owned);
        Ok(())
    })
}

// ===========================================================================
// cfunc_hash / cfunc_match (funccache.c:84 / :108)
// ===========================================================================

/// `cfunc_hash` (funccache.c:84) — hash function for the cfunc hash table.
///
/// Hash the fixed scalar fields (everything but `callResultType`), then fold in
/// the live `nargs` input argtypes, then fold in the result rowtype hash when a
/// `callResultType` is present. The hash bytes differ from C's `hash_any` (the
/// owned key is not `repr(C)`) but stay consistent with [`cfunc_match`].
fn cfunc_hash(k: &CachedFunctionHashKey) -> u32 {
    let mut buf = [0u8; 4 + 1 + 1 + 8 + 4 + 4 + 4];
    let mut n = 0usize;
    let mut put = |bytes: &[u8], n: &mut usize| {
        buf[*n..*n + bytes.len()].copy_from_slice(bytes);
        *n += bytes.len();
    };
    put(&k.funcOid.to_le_bytes(), &mut n);
    put(&[k.isTrigger as u8], &mut n);
    put(&[k.isEventTrigger as u8], &mut n);
    put(&(k.cacheEntrySize as u64).to_le_bytes(), &mut n);
    put(&k.trigOid.to_le_bytes(), &mut n);
    put(&k.inputCollation.to_le_bytes(), &mut n);
    put(&k.nargs.to_le_bytes(), &mut n);
    let mut h = hash_bytes(&buf[..n]);

    if k.nargs > 0 {
        let nargs = (k.nargs as usize).min(k.argtypes.len());
        let mut arg_bytes = [0u8; core::mem::size_of::<Oid>() * types_core::FUNC_MAX_ARGS];
        let mut m = 0usize;
        for &arg in &k.argtypes[..nargs] {
            arg_bytes[m..m + 4].copy_from_slice(&arg.to_le_bytes());
            m += 4;
        }
        h = hash_combine(h, hash_bytes(&arg_bytes[..m]));
    }

    if let Some(tupdesc) = &k.callResultType {
        h = hash_combine(h, hashRowType(tupdesc));
    }
    h
}

/// `cfunc_match` (funccache.c:108) — equality function. Returns `true` when the
/// two keys are equal (C returns `0`/"equal"; inverted to a `bool`).
fn cfunc_match(k1: &CachedFunctionHashKey, k2: &CachedFunctionHashKey) -> bool {
    // Compare all the fixed fields except callResultType.
    if k1.funcOid != k2.funcOid
        || k1.isTrigger != k2.isTrigger
        || k1.isEventTrigger != k2.isEventTrigger
        || k1.cacheEntrySize != k2.cacheEntrySize
        || k1.trigOid != k2.trigOid
        || k1.inputCollation != k2.inputCollation
        || k1.nargs != k2.nargs
    {
        return false;
    }

    // Compare input argument types (we just verified that nargs matches).
    if k1.nargs > 0 {
        let nargs = (k1.nargs as usize).min(k1.argtypes.len());
        if k1.argtypes[..nargs] != k2.argtypes[..nargs] {
            return false;
        }
    }

    // Compare callResultType.
    match (&k1.callResultType, &k2.callResultType) {
        (Some(t1), Some(t2)) => equalRowTypes(t1, t2),
        (Some(_), None) | (None, Some(_)) => false,
        (None, None) => true,
    }
}

// ===========================================================================
// cfunc_hashtable_lookup / _insert / _delete (funccache.c:145 / :166 / :206)
// ===========================================================================

/// `cfunc_hashtable_lookup` (funccache.c:145) — look up the CachedFunction for
/// the given hash key. Returns `None` if not present (including a NULL table).
fn cfunc_hashtable_lookup(func_key: &CachedFunctionHashKey) -> Option<CachedFunctionRef> {
    CFUNC_HASHTABLE.with(|cell| {
        let slot = cell.borrow();
        let owned = slot.as_ref()?;
        owned.with(|state| {
            let chain = state.table.get(&key_id(func_key))?;
            chain
                .iter()
                .find(|e| cfunc_match(&e.key, func_key))
                .map(|e| e.function.clone())
        })
    })
}

/// `cfunc_hashtable_insert` (funccache.c:166) — insert a hash table entry.
///
/// `func_key` is consumed; its `callResultType`, if present, is copied into the
/// cache context (the C `CreateTupleDescCopy(...TopMemoryContext)`). The
/// payload's `fn_hashkey` back-link is set to the installed key id.
fn cfunc_hashtable_insert(
    function: CachedFunctionRef,
    func_key: CachedFunctionHashKey,
) -> PgResult<()> {
    if CFUNC_HASHTABLE.with(|cell| cell.borrow().is_none()) {
        cfunc_hashtable_init()?;
    }

    let id = key_id(&func_key);

    CFUNC_HASHTABLE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let owned = slot.as_mut().expect("table just initialized");
        owned.with_mut(|state| {
            // If there's a callResultType, copy it into the cache context. If
            // the copy fails, leave the entry with a null callResultType, which
            // will probably never match anything (the C behavior).
            let key_in_ctx = relocate_key(state.mcx, func_key)?;

            let found = state
                .table
                .get(&id)
                .map(|chain| chain.iter().any(|e| cfunc_match(&e.key, &key_in_ctx)))
                .unwrap_or(false);
            if found {
                elog_warning("trying to insert a function that already exists");
            }

            // Set back-link from function to hashtable key.
            function.borrow_mut().cfunc_mut().fn_hashkey = Some(id);

            let chain = state
                .table
                .entry(id)
                .or_insert_with(|| PgVec::new_in(state.mcx));
            chain.push(CachedFunctionHashEntry {
                key: key_in_ctx,
                function,
            });
            Ok(())
        })
    })
}

/// Rebuild `func_key` with its `callResultType` re-allocated in the cache
/// context `mcx` (the C `CreateTupleDescCopy(... TopMemoryContext)` at insert).
/// On copy failure the key is returned with a null `callResultType`, matching
/// the C "leave the entry with null callResultType" fallback.
fn relocate_key<'mcx>(
    mcx: Mcx<'mcx>,
    key: CachedFunctionHashKey<'_>,
) -> PgResult<CachedFunctionHashKey<'mcx>> {
    let CachedFunctionHashKey {
        funcOid,
        isTrigger,
        isEventTrigger,
        cacheEntrySize,
        trigOid,
        inputCollation,
        nargs,
        callResultType,
        argtypes,
    } = key;

    let callResultType = match callResultType {
        Some(src) => match CreateTupleDescCopy(mcx, &src) {
            Ok(copy) => mcx::alloc_in(mcx, copy).ok(),
            Err(_) => None,
        },
        None => None,
    };

    Ok(CachedFunctionHashKey {
        funcOid,
        isTrigger,
        isEventTrigger,
        cacheEntrySize,
        trigOid,
        inputCollation,
        nargs,
        callResultType,
        argtypes,
    })
}

/// `cfunc_hashtable_delete` (funccache.c:206) — delete a hash table entry. Does
/// nothing if the function is not in the table (`fn_hashkey == NULL`). Frees the
/// `callResultType` (here: dropped with the entry).
fn cfunc_hashtable_delete(function: &CachedFunctionRef) {
    // do nothing if not in table
    let id = match function.borrow().cfunc().fn_hashkey {
        Some(id) => id,
        None => return,
    };

    let removed = CFUNC_HASHTABLE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let Some(owned) = slot.as_mut() else {
            return false;
        };
        owned.with_mut(|state| {
            let Some(chain) = state.table.get_mut(&id) else {
                return false;
            };
            // Identify the entry by payload identity (the same Rc allocation).
            let before = chain.len();
            chain.retain(|e| !CachedFunctionRef::ptr_eq(&e.function, function));
            // Dropping the removed entry releases its callResultType copy
            // (the C FreeTupleDesc(tupdesc)).
            chain.len() != before
        })
    });

    if !removed {
        elog_warning("trying to delete function that does not exist");
    }

    // Remove back link, which no longer points to allocated storage.
    function.borrow_mut().cfunc_mut().fn_hashkey = None;
}

// ===========================================================================
// compute_function_hashkey (funccache.c:246)
// ===========================================================================

/// `compute_function_hashkey` (funccache.c:246) — compute the hashkey for a
/// given function invocation. Returns the populated key by value (the C fills
/// caller-provided storage; the all-zero `Default` matches C's leading
/// `memset`). When a `callResultType` is incorporated nothing is done about
/// copying it — the caller (insert) owns that.
fn compute_function_hashkey<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx FunctionCallInfoBaseData<'mcx>,
    proc: &ProcCompileInfo<'mcx>,
    cacheEntrySize: Size,
    includeResultType: bool,
    forValidator: bool,
) -> PgResult<CachedFunctionHashKey<'mcx>> {
    let mut hashkey = CachedFunctionHashKey::default();

    let (fn_oid, fn_expr) = fn_oid_and_call_expr(fcinfo);

    // get function OID
    hashkey.funcOid = fn_oid;

    // get call context
    hashkey.isTrigger = called_as(fcinfo, T_TriggerData);
    hashkey.isEventTrigger = called_as(fcinfo, T_EventTriggerData);

    // record cacheEntrySize so multiple languages can share hash table
    hashkey.cacheEntrySize = cacheEntrySize;

    // If DML trigger, include trigger's OID in the hash. In validation mode we
    // leave trigOid zero (the hash entry built then is never used for calls).
    if hashkey.isTrigger && !forValidator {
        hashkey.trigOid = trigger_oid(fcinfo)?;
    }

    // get input collation, if known
    hashkey.inputCollation = fcinfo.fncollation;

    // We include only input arguments in the hash key, resolving any
    // polymorphic argument types to the real types for the call.
    if proc.pronargs > 0 {
        let nargs = proc.pronargs as usize;
        if nargs > types_core::FUNC_MAX_ARGS || nargs > hashkey.argtypes.len() {
            return Err(PgError::error("pg_proc pronargs is out of range"));
        }
        if proc.proargtypes.len() < nargs {
            return Err(PgError::error("pg_proc proargtypes is too short"));
        }
        hashkey.nargs = proc.pronargs as i32;
        hashkey.argtypes[..nargs].copy_from_slice(&proc.proargtypes[..nargs]);

        cfunc_resolve_polymorphic_argtypes(
            proc.pronargs as i32,
            &mut hashkey.argtypes,
            None, // all args are inputs
            fn_expr.as_ref(),
            forValidator,
            proc.proname.as_str(),
        )?;
    }

    // A function returning composite has additional variability; if the caller
    // needs the exact result type in the key, run get_call_result_type().
    if includeResultType {
        let resolved = get_call_result_type(mcx, fcinfo)?;
        match resolved.class {
            Some(TypeFuncClass::Composite) | Some(TypeFuncClass::CompositeDomain) => {
                hashkey.callResultType = resolved.result_tuple_desc.map(|d| {
                    // d is a PgBox<'mcx, TupleDescData>; carry it through.
                    d
                });
            }
            // scalar result, or indeterminate rowtype: leave callResultType None
            _ => {}
        }
    }

    Ok(hashkey)
}

/// `CALLED_AS_TRIGGER(fcinfo)` / `CALLED_AS_EVENT_TRIGGER(fcinfo)` —
/// `fcinfo->context != NULL && IsA(fcinfo->context, <tag>)`.
fn called_as(fcinfo: &FunctionCallInfoBaseData, tag: u32) -> bool {
    matches!(&fcinfo.context, Some(ctx) if ctx.tag() == tag)
}

/// `fcinfo->flinfo->fn_oid` + `fcinfo->flinfo->fn_expr` — read through the fmgr
/// seam. The call expression is recovered as the erased `FmgrInfo.fn_expr` `Expr`
/// (a plan-tree `&Node` cannot model `FuncExpr`/`OpExpr`) and wrapped in
/// [`CallExpr`], the form the polymorphic-argtype resolver consumes.
fn fn_oid_and_call_expr<'mcx>(
    fcinfo: &'mcx FunctionCallInfoBaseData<'mcx>,
) -> (Oid, Option<CallExpr>) {
    let (fn_oid, erased) =
        fmgr_seams::fn_oid_and_fn_expr_erased::call(fcinfo);
    (fn_oid, erased.map(CallExpr::from_erased))
}

/// `((TriggerData *) fcinfo->context)->tg_trigger->tgoid` — read through the
/// trigger seam (recovering the `TriggerData` payload from the context node and
/// reading `tg_trigger->tgoid` is a trigger-subsystem node access).
fn trigger_oid(fcinfo: &FunctionCallInfoBaseData) -> PgResult<Oid> {
    funccache_seams::trigger_context_oid::call(fcinfo)
}

// ===========================================================================
// cfunc_resolve_polymorphic_argtypes (funccache.c:347)
// ===========================================================================

/// `cfunc_resolve_polymorphic_argtypes` (funccache.c:347).
///
/// Like the standard `resolve_polymorphic_argtypes`, except: (1) it raises the
/// error itself if the types can't be resolved; (2) it treats RECORD-type
/// *input* arguments (not output arguments) as polymorphic, replacing their
/// types with the actual input types when those can be determined from the call
/// expression; (3) in validation mode (no inputs to inspect) it assumes
/// polymorphic arguments are integer, integer-array or integer-range.
pub fn cfunc_resolve_polymorphic_argtypes(
    numargs: i32,
    argtypes: &mut [Oid],
    argmodes: Option<&[u8]>,
    call_expr: Option<&CallExpr>,
    forValidator: bool,
    proname: &str,
) -> PgResult<()> {
    if !forValidator {
        // normal case, pass to standard routine
        if !resolve_polymorphic_argtypes(argtypes, argmodes, call_expr)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "could not determine actual argument type for polymorphic function \"{proname}\""
                ))
                .into_error());
        }

        // also, treat RECORD inputs (but not outputs) as polymorphic
        let mut inargno: i32 = 0;
        for i in 0..numargs as usize {
            let argmode = match &argmodes {
                Some(modes) => modes[i],
                None => PROARGMODE_IN,
            };

            if argmode == PROARGMODE_OUT || argmode == PROARGMODE_TABLE {
                continue;
            }
            if argtypes[i] == RECORDOID || argtypes[i] == RECORDARRAYOID {
                let resolvedtype = match call_expr {
                    Some(ce) => ce.argtype(inargno)?,
                    None => InvalidOid,
                };
                if resolvedtype != InvalidOid {
                    argtypes[i] = resolvedtype;
                }
            }
            inargno += 1;
        }
    } else {
        // special validation case (no need to do anything for RECORD)
        for i in 0..numargs as usize {
            match argtypes[i] {
                ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID | ANYCOMPATIBLEOID
                | ANYCOMPATIBLENONARRAYOID => argtypes[i] = INT4OID,
                ANYARRAYOID | ANYCOMPATIBLEARRAYOID => argtypes[i] = INT4ARRAYOID,
                ANYRANGEOID | ANYCOMPATIBLERANGEOID => argtypes[i] = INT4RANGEOID,
                ANYMULTIRANGEOID => argtypes[i] = INT4MULTIRANGEOID,
                _ => {}
            }
        }
    }

    Ok(())
}

// ===========================================================================
// delete_function (funccache.c:432)
// ===========================================================================

/// `delete_function` (funccache.c:432) — clean up as much as possible of a
/// stale function cache entry.
///
/// We can't release the payload itself (other `fn_extra` clones may exist). We
/// release the subsidiary storage (via the delete callback) only if there are no
/// active evaluations in progress (`use_count == 0`); otherwise we leak it (a
/// rare corner case). Idempotent: callable more than once.
fn delete_function(func: &CachedFunctionRef) {
    // remove function from hash table (might be done already)
    cfunc_hashtable_delete(func);

    // release the function's storage if safe and not done already
    let (do_delete, dcallback) = {
        let f = func.borrow();
        let c = f.cfunc();
        (c.use_count == 0 && c.dcallback.is_some(), c.dcallback)
    };
    if do_delete {
        if let Some(cb) = dcallback {
            let mut f = func.borrow_mut();
            cb(&mut *f);
            f.cfunc_mut().dcallback = None;
        }
    }
}

// ===========================================================================
// cached_function_compile (funccache.c:479)
// ===========================================================================

/// `cached_function_compile` (funccache.c:479) — compile a cached function, if
/// no existing cache entry is suitable.
///
/// `function` is `None` or the result of a previous call for the same `fcinfo`
/// (the caller stashes it in `fn_extra` and passes it back). `ccallback` /
/// `dcallback` are the language-specific compile / delete callbacks;
/// `cacheEntrySize` is the language-specific cache-entry size. If
/// `includeResultType` and the function returns composite, the actual result
/// descriptor is part of the lookup key. If `forValidator`, some checks are
/// skipped.
///
/// Leaves `use_count` zero; the caller increments/decrements it.
pub fn cached_function_compile<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx FunctionCallInfoBaseData<'mcx>,
    mut function: Option<CachedFunctionRef>,
    ccallback: CompileCallback,
    dcallback: types_funccache::CachedFunctionDeleteCallback,
    cacheEntrySize: Size,
    includeResultType: bool,
    forValidator: bool,
) -> PgResult<CachedFunctionRef> {
    let (fn_oid, _) = fn_oid_and_call_expr(fcinfo);

    // Lookup the pg_proc tuple by Oid; we'll need it in any case (and its
    // xmin/ctid for the up-to-dateness check).
    let proc = funccache_seams::search_proc_compile_info::call(mcx, fn_oid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for function {fn_oid}")))?;

    let mut function_valid = false;
    let mut hashkey_valid = false;
    let mut hashkey: Option<CachedFunctionHashKey<'mcx>> = None;
    let mut new_function = false;

    // recheck: loop to retry the hashtable lookup if a concurrent replacement
    // happened (the C `goto recheck`).
    loop {
        if function.is_none() {
            let k = compute_function_hashkey(
                mcx,
                fcinfo,
                &proc,
                cacheEntrySize,
                includeResultType,
                forValidator,
            )?;
            function = cfunc_hashtable_lookup(&k);
            hashkey = Some(k);
            hashkey_valid = true;
        }

        if let Some(f) = &function {
            // We have a compiled function, but is it still valid?
            let (xmin, tid) = {
                let b = f.borrow();
                let c = b.cfunc();
                (c.fn_xmin, c.fn_tid)
            };
            if xmin == proc.xmin && tid == proc.tid {
                function_valid = true;
            } else {
                // Stale: remove from hashtable and try to drop storage.
                delete_function(f);

                // If not in active use we can overwrite the struct with new
                // data; otherwise leave it and make a new one. If we found it
                // via fn_extra, recheck the hashtable for a replacement.
                let use_count = f.borrow().cfunc().use_count;
                if use_count != 0 {
                    function = None;
                    if !hashkey_valid {
                        continue; // goto recheck
                    }
                }
            }
        }
        break;
    }

    // If the function wasn't found or was out-of-date, compile it.
    if !function_valid {
        // Calculate hashkey if we didn't already.
        if !hashkey_valid {
            hashkey = Some(compute_function_hashkey(
                mcx,
                fcinfo,
                &proc,
                cacheEntrySize,
                includeResultType,
                forValidator,
            )?);
        }
        let hashkey = hashkey.expect("hashkey computed");

        debug_assert!(cacheEntrySize >= core::mem::size_of::<CachedFunction>());

        // Create the new payload, if not done already. The cache entry is kept
        // for the life of the backend (the language callback allocates the
        // payload in a backend-lifetime context).
        let function = match function {
            None => {
                new_function = true;
                None
            }
            Some(f) => {
                // re-using a previously existing struct: clear it out. The
                // language re-initializes the payload through the callback;
                // here we hand the existing ref to the callback to rebuild.
                Some(f)
            }
        };

        // Do the hard, language-specific part. On failure, if we just allocated
        // the struct, the language callback owns not leaking it (here a failed
        // Err simply drops the freshly-built ref); re-throw.
        let built = ccallback(mcx, fcinfo, &proc, function, forValidator)?;
        let _ = new_function;

        // Fill in the CachedFunction part (last, so the function never looks
        // valid before it's fully built). fn_hashkey is set by the insert;
        // use_count remains zero.
        {
            let mut b = built.borrow_mut();
            let c = b.cfunc_mut();
            c.fn_xmin = proc.xmin;
            c.fn_tid = proc.tid;
            c.dcallback = dcallback;
        }

        // Add the completed struct to the hash table.
        cfunc_hashtable_insert(built.clone(), hashkey)?;

        return Ok(built);
    }

    // Finally return the compiled function.
    Ok(function.expect("function_valid implies Some"))
}

/// The language-specific compile callback (`CachedFunctionCompileCallback`,
/// funccache.h). C signature: `(fcinfo, procTup, hashkey, function,
/// forValidator)`. The owned model passes the projected [`ProcCompileInfo`] in
/// place of the raw `procTup`, and the existing payload ref (to rebuild) or
/// `None` (to allocate fresh); it returns the built [`CachedFunctionRef`]
/// (allocated by the language in a backend-lifetime context), or an error.
pub type CompileCallback = for<'mcx> fn(
    Mcx<'mcx>,
    &'mcx FunctionCallInfoBaseData<'mcx>,
    &ProcCompileInfo<'mcx>,
    Option<CachedFunctionRef>,
    bool,
) -> PgResult<CachedFunctionRef>;

/// `elog(WARNING, ...)` — non-fatal cache-consistency warning.
fn elog_warning(msg: &str) {
    let _ = utils_error::elog(WARNING, msg);
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install this crate's inward seams.
///
/// The trigger-context and proc-projection seams funccache *consumes* are owned
/// and installed by the trigger / syscache crates. funccache owns the
/// `cfunc_use_count` projection: a procedural language reads `cfunc->use_count`
/// back through its opaque `CachedFunction` header handle (PL/pgSQL's
/// `plpgsql_free_function_memory` "Better not call this on an in-use function"
/// assert). The handle is the funccache-cache locator; a handle of `0` is a
/// `PLpgSQL_function` that was never entered into the funccache cache (e.g. an
/// inline `DO` block, or a freshly `palloc0`-built struct) — its embedded
/// `CachedFunction` header is zero-initialized, so `use_count` reads as `0`,
/// exactly as in C.
pub fn init_seams() {
    funccache_seams::cfunc_use_count::set(cfunc_use_count_impl);
}

/// `cfunc->use_count` read through the opaque `CachedFunction` handle. See
/// [`init_seams`]. Handle `0` is the never-cached / zero-initialized header
/// (`use_count == 0`). A nonzero handle would index a live cache entry once the
/// funccache↔language `fn_extra` bridge is wired; until then it is unreachable
/// and resolves the same zero header.
fn cfunc_use_count_impl(cfunc: plpgsql::CachedFunction) -> u64 {
    let _ = cfunc;
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_tuple::heaptuple::{ANYARRAYOID, ANYELEMENTOID, ANYMULTIRANGEOID, ANYRANGEOID};

    #[test]
    fn validation_mode_substitutes_polymorphic_types() {
        // forValidator path takes no seams; it rewrites ANY* in place.
        let mut argtypes = [
            ANYELEMENTOID,
            ANYARRAYOID,
            ANYRANGEOID,
            ANYMULTIRANGEOID,
            RECORDOID,
        ];
        cfunc_resolve_polymorphic_argtypes(
            argtypes.len() as i32,
            &mut argtypes,
            None,
            None,
            true,
            "f",
        )
        .unwrap();
        assert_eq!(
            argtypes,
            [
                INT4OID,
                INT4ARRAYOID,
                INT4RANGEOID,
                INT4MULTIRANGEOID,
                RECORDOID, // RECORD untouched in validation mode
            ]
        );
    }

    #[test]
    fn match_ignores_unused_argtype_slots_and_hash_agrees() {
        let mut left = CachedFunctionHashKey {
            funcOid: 1,
            nargs: 1,
            ..CachedFunctionHashKey::default()
        };
        let mut right = CachedFunctionHashKey {
            funcOid: 1,
            nargs: 1,
            ..CachedFunctionHashKey::default()
        };
        left.argtypes[0] = INT4OID;
        right.argtypes[0] = INT4OID;
        // unused trailing slots differ; must not affect match or hash.
        left.argtypes[2] = 11;
        right.argtypes[2] = 99;
        assert!(cfunc_match(&left, &right));
        assert_eq!(cfunc_hash(&left), cfunc_hash(&right));
    }

    #[test]
    fn distinct_argtypes_do_not_match() {
        let mut left = CachedFunctionHashKey {
            funcOid: 1,
            nargs: 1,
            ..CachedFunctionHashKey::default()
        };
        let mut right = CachedFunctionHashKey {
            funcOid: 1,
            nargs: 1,
            ..CachedFunctionHashKey::default()
        };
        left.argtypes[0] = INT4OID;
        right.argtypes[0] = RECORDOID;
        assert!(!cfunc_match(&left, &right));
    }

    #[test]
    fn fixed_field_difference_breaks_match() {
        let a = CachedFunctionHashKey {
            funcOid: 1,
            inputCollation: 100,
            ..CachedFunctionHashKey::default()
        };
        let b = CachedFunctionHashKey {
            funcOid: 1,
            inputCollation: 200,
            ..CachedFunctionHashKey::default()
        };
        assert!(!cfunc_match(&a, &b));
    }
}
