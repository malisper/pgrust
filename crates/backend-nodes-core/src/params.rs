//! Family: **params** — `nodes/params.c`, the `ParamListInfo` machinery.
//!
//! Ports `makeParamList`, `copyParamList`, `EstimateParamListSpace`,
//! `SerializeParamList`, `RestoreParamList`, `BuildParamLogString`,
//! `ParamsErrorCallback`, and the parser hooks `paramlist_parser_setup` /
//! `paramlist_param_ref`.
//!
//! ## Handle-backed store
//!
//! A live `ParamListInfo` crosses crate boundaries as the opaque
//! [`ParamListInfoHandle`] (`types_nodes::parsestmt`). This family backs each
//! handle with a real [`ParamListInfoData`] (`types_nodes::params`, mirrored
//! field-for-field from `nodes/params.h`) in a backend-local
//! ([`thread_local!`]) handle-keyed store — the same "real entry store" idiom
//! the relcache / portalmem units use. `makeParamList` allocates a new entry and
//! returns its handle; the remaining operations look the entry up by handle and
//! operate on the owned struct. C's `palloc`'d flexible-array `params[]` is the
//! struct's `Vec<ParamExternData>`; C's `char **start_address` serialize cursor
//! is a raw `*mut u8` cursor (the `datum.c` seam contract), advanced exactly as
//! the C advances `*start_address`.
//!
//! ## Seams (genuinely unported owners)
//!
//! Cross-subsystem calls route through existing owner seams:
//!
//! * `datum.c` — `datum_copy` / `datum_estimate_space` / `datum_serialize` /
//!   `datum_restore` (`backend-utils-adt-datum-seams`; owner
//!   `backend-utils-adt-datum`).
//! * `lsyscache.c` — `get_typlenbyval` / `get_typcollation` /
//!   `get_type_output_info` (`backend-utils-cache-lsyscache-seams`).
//! * `fmgr.c` — `oid_output_function_call_datum`
//!   (`backend-utils-fmgr-fmgr-seams`).
//! * `xact.c` — `is_aborted_transaction_block_state`
//!   (`backend-access-transam-xact-seams`).
//!
//! The owned `make_param_list` seam in `backend-nodes-params-seams` is installed
//! by this crate's `init_seams()`.

#![allow(non_snake_case)]

use std::cell::RefCell;
use std::collections::HashMap;

use mcx::{Mcx, MemoryContext};
use types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY};
use types_nodes::params::{
    ParamExternData, ParamListInfoData, ParamRef, ParamsErrorCbData, T_Param,
};
use types_nodes::parsestmt::ParamListInfoHandle;
use types_nodes::primnodes::{Param, PARAM_EXTERN};

use backend_access_transam_xact_seams as xact_seam;
use backend_utils_adt_datum_seams as datum_seam;
use backend_utils_cache_lsyscache_seams as lsyscache_seam;
use backend_utils_fmgr_fmgr_seams as fmgr_seam;
use types_core::Oid;
// The canonical unified value type (Datum-unification keystone). The owned
// `ParamListInfoData`/`ParamExternData` carry `Datum<'mcx>`; the handle-keyed
// store backs them in a backend-lifetime context, so the stored value type is
// `Datum<'static>` (see `PARAM_LIST_CONTEXT`).
use types_tuple::backend_access_common_heaptuple::Datum;

/// `sizeof(int)` — the 4-byte count word written/read by the serializer
/// (`memcpy(..., &nparams, sizeof(int))`).
const SIZEOF_INT: usize = core::mem::size_of::<i32>();
/// `sizeof(Oid)` — 4 bytes.
const SIZEOF_OID: usize = core::mem::size_of::<u32>();
/// `sizeof(uint16)` — the `pflags` field width.
const SIZEOF_UINT16: usize = core::mem::size_of::<u16>();

/// `OidIsValid(oid)` (`postgres_ext.h`): a valid (non-`InvalidOid`) OID.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != 0
}

// ===========================================================================
// Handle-keyed store
// ===========================================================================

thread_local! {
    /// The backend-local `ParamListInfo` store. The C `ParamListInfo` pointer is
    /// the [`ParamListInfoHandle`] key; the palloc'd object is the owned value.
    ///
    /// The stored value carries `Datum<'static>`: the C objects are palloc'd in a
    /// long-lived (backend-lifetime) context, modeled by [`param_list_mcx`].
    static PARAM_LISTS: RefCell<HashMap<u64, ParamListInfoData<'static>>> =
        RefCell::new(HashMap::new());
    /// Monotonic handle allocator (`1..`; `0` is reserved for the C `NULL`
    /// `ParamListInfoHandle::NULL`).
    static NEXT_HANDLE: RefCell<u64> = const { RefCell::new(1) };
    /// The backend-lifetime memory context backing the store's by-reference
    /// datum images. C's `copyParamList` / `RestoreParamList` `datumCopy` into a
    /// caller context that outlives the `ParamListInfo`; the owned model copies
    /// into this leaked, never-reset context so the stored `Datum<'static>`
    /// by-reference bytes stay valid for the store's lifetime.
    static PARAM_LIST_CONTEXT: &'static MemoryContext =
        Box::leak(Box::new(MemoryContext::new("ParamListInfo")));
}

/// `Mcx<'static>` for the backend-lifetime [`PARAM_LIST_CONTEXT`] — where the
/// store's owned by-reference datum images are allocated.
fn param_list_mcx() -> Mcx<'static> {
    PARAM_LIST_CONTEXT.with(|c| c.mcx())
}

/// Allocate the next non-NULL handle id.
fn alloc_handle() -> u64 {
    NEXT_HANDLE.with(|n| {
        let mut n = n.borrow_mut();
        let h = *n;
        *n += 1;
        h
    })
}

/// Run `f` over the owned `ParamListInfoData` behind `handle` (read-only).
fn with_list<R>(
    handle: ParamListInfoHandle,
    f: impl FnOnce(&ParamListInfoData<'static>) -> R,
) -> R {
    PARAM_LISTS.with(|store| {
        let store = store.borrow();
        let pl = store
            .get(&handle.0)
            .expect("ParamListInfoHandle refers to a live ParamListInfo");
        f(pl)
    })
}

/// Run `f` over the owned `ParamListInfoData` behind `handle` (mutable).
fn with_list_mut<R>(
    handle: ParamListInfoHandle,
    f: impl FnOnce(&mut ParamListInfoData<'static>) -> R,
) -> R {
    PARAM_LISTS.with(|store| {
        let mut store = store.borrow_mut();
        let pl = store
            .get_mut(&handle.0)
            .expect("ParamListInfoHandle refers to a live ParamListInfo");
        f(pl)
    })
}

/// Install a freshly built `ParamListInfoData` into the store and return its
/// handle.
fn install(pl: ParamListInfoData<'static>) -> ParamListInfoHandle {
    let h = alloc_handle();
    PARAM_LISTS.with(|store| store.borrow_mut().insert(h, pl));
    ParamListInfoHandle(h)
}

/// Snapshot the owned struct behind `handle` (used where the C reads `*paramLI`
/// out of the store before re-entering it under a fresh allocation).
fn snapshot(handle: ParamListInfoHandle) -> ParamListInfoData<'static> {
    with_list(handle, |pl| pl.clone())
}

// ===========================================================================
// makeParamList
// ===========================================================================

/// `makeParamList(numParams)` (params.c): allocate and initialize a new
/// `ParamListInfo` with room for `numParams` `ParamExternData` slots.
///
/// To make a structure for the "dynamic" way (with hooks), pass 0 for
/// `numParams` and set it manually. C supplies a default `parserSetup`
/// (`paramlist_parser_setup`) automatically; the owned model records only that a
/// parser setup is present and the parser installs the concrete resolver
/// ([`paramlist_param_ref`]) explicitly (see the module docs).
pub fn makeParamList(num_params: i32) -> PgResult<ParamListInfoHandle> {
    // size = offsetof(ParamListInfoData, params)
    //        + numParams * sizeof(ParamExternData);
    // modeled by allocating `numParams` empty slots (zero when non-positive,
    // as a Vec length cannot be negative).
    let count = if num_params > 0 { num_params as usize } else { 0 };

    let mut params = Vec::new();
    params
        .try_reserve_exact(count)
        .map_err(|_| {
            PgError::error("out of memory while allocating a parameter list")
                .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
        })?;
    for _ in 0..count {
        params.push(ParamExternData::empty());
    }

    let pl = ParamListInfoData {
        param_fetch: false,
        param_fetch_arg: None,
        param_compile: false,
        param_compile_arg: None,
        // C: parserSetup = paramlist_parser_setup; parserSetupArg = retval.
        parser_setup: true,
        parser_setup_arg: None,
        param_values_str: None,
        num_params,
        params,
    };

    Ok(install(pl))
}

// ===========================================================================
// copyParamList
// ===========================================================================

/// `copyParamList(from)` (params.c): make a static, self-contained copy of a
/// `ParamListInfo`, `datumCopy`-ing pass-by-reference values into the current
/// context. Dynamic hooks and `paramValuesStr` are intentionally not copied.
///
/// Returns `ParamListInfoHandle::NULL` when `from` is NULL or has
/// `numParams <= 0` (C's `return NULL`).
pub fn copyParamList(from: ParamListInfoHandle) -> PgResult<ParamListInfoHandle> {
    if from == ParamListInfoHandle::NULL {
        return Ok(ParamListInfoHandle::NULL);
    }
    let from = snapshot(from);
    if from.num_params <= 0 {
        return Ok(ParamListInfoHandle::NULL);
    }

    let retval = makeParamList(from.num_params)?;

    for i in 0..from.num_params as usize {
        // give hook a chance in case parameter is dynamic
        let oprm = if from.param_fetch {
            paramFetch(&from, (i + 1) as i32)
        } else {
            from.params[i].clone()
        };

        // flat-copy the parameter info
        let mut nprm = oprm;

        // need datumCopy in case it's a pass-by-reference datatype
        if !(nprm.isnull || !oid_is_valid(nprm.ptype)) {
            let (typ_len, typ_byval) = lsyscache_seam::get_typlenbyval::call(nprm.ptype)?;
            nprm.value =
                datum_seam::datum_copy_v::call(param_list_mcx(), &nprm.value, typ_byval, typ_len as i32)?;
        }

        with_list_mut(retval, |pl| pl.params[i] = nprm);
    }

    Ok(retval)
}

// ===========================================================================
// paramlist_parser_setup / paramlist_param_ref (parser hooks)
// ===========================================================================

/// `paramlist_parser_setup(pstate, arg)` (params.c, static): set up to parse a
/// query referencing parameters sourced from a `ParamListInfo`.
///
/// In C this writes `pstate->p_paramref_hook = paramlist_param_ref;` and
/// `pstate->p_ref_hook_state = arg;`. The owned `ParseState` models those hook
/// slots opaquely, so the parser wires [`paramlist_param_ref`] in directly with
/// the `ParamListInfo` as the resolver's argument; this is the documented marker
/// of that contract (the resolver and its argument are the real artifacts). The
/// `p_coerce_param_hook` is left unset, exactly as C does.
pub fn paramlist_parser_setup(_arg: ParamListInfoHandle) {
    // no need to use p_coerce_param_hook
}

/// `paramlist_param_ref(pstate, pref)` (params.c, static): transform a
/// [`ParamRef`] using parameter type data from the `ParamListInfo`
/// (`pstate->p_ref_hook_state`). Returns a freshly made [`Param`] node, or
/// `None` if the parameter number is out of range or the parameter has no type
/// (C's `return NULL`).
pub fn paramlist_param_ref(
    param_li: ParamListInfoHandle,
    pref: &ParamRef,
) -> PgResult<Option<Param>> {
    let param_li = snapshot(param_li);
    let paramno = pref.number;

    // check parameter number is valid
    if paramno <= 0 || paramno > param_li.num_params {
        return Ok(None);
    }

    // give hook a chance in case parameter is dynamic
    let prm = if param_li.param_fetch {
        paramFetch(&param_li, paramno)
    } else {
        param_li.params[(paramno - 1) as usize].clone()
    };

    if !oid_is_valid(prm.ptype) {
        return Ok(None);
    }

    // param = makeNode(Param);
    let paramtype = prm.ptype;
    let param = Param {
        // `Param`'s leading `Expr xpr` carries the T_Param tag in C; the owned
        // `Param` carries only the post-`Expr` fields, so the tag is implicit.
        paramkind: PARAM_EXTERN,
        paramid: paramno,
        paramtype,
        paramtypmod: -1,
        paramcollid: lsyscache_seam::get_typcollation::call(paramtype)?,
        // param->location = pref->location;
        location: pref.location,
    };
    let _ = T_Param;

    Ok(Some(param))
}

/// The dynamic `paramFetch` hook path: in C `paramLI->paramFetch(paramLI, id,
/// false, &workspace)` fetches one dynamic param into a stack workspace. The
/// owned model carries the hook in another subsystem and never installs one
/// itself (`makeParamList` leaves `param_fetch == false`), so reaching here
/// means a caller set `param_fetch` without an owner having landed.
fn paramFetch(_param_li: &ParamListInfoData<'static>, _paramid: i32) -> ParamExternData<'static> {
    panic!(
        "params: dynamic ParamListInfo paramFetch hook invoked, but no paramFetch owner is \
         ported (the hook function pointer lives in an unported subsystem)"
    )
}

// ===========================================================================
// EstimateParamListSpace
// ===========================================================================

/// `EstimateParamListSpace(paramLI)` (params.c): estimate the bytes needed to
/// serialize a `ParamListInfo` (4-byte count, then per-param OID / flags /
/// datum).
pub fn EstimateParamListSpace(param_li: ParamListInfoHandle) -> PgResult<usize> {
    let mut sz: usize = SIZEOF_INT;

    if param_li == ParamListInfoHandle::NULL {
        return Ok(sz);
    }
    let param_li = snapshot(param_li);
    if param_li.num_params <= 0 {
        return Ok(sz);
    }

    for i in 0..param_li.num_params as usize {
        // give hook a chance in case parameter is dynamic
        let prm = if param_li.param_fetch {
            paramFetch(&param_li, (i + 1) as i32)
        } else {
            param_li.params[i].clone()
        };

        let type_oid = prm.ptype;

        sz = add_size(sz, SIZEOF_OID)?; // space for type OID
        sz = add_size(sz, SIZEOF_UINT16)?; // space for pflags

        // space for datum/isnull
        let (typ_len, typ_byval) = typlenbyval_or_assumed(type_oid)?;
        sz = add_size(
            sz,
            datum_seam::datum_estimate_space_v::call(
                &prm.value,
                prm.isnull,
                typ_byval,
                typ_len as i32,
            ),
        )?;
    }

    Ok(sz)
}

/// `get_typlenbyval(typeOid, ...)` when `OidIsValid(typeOid)`, else the
/// "assume by-value, like copyParamList does" fallback `{ sizeof(Datum), true }`
/// shared by `EstimateParamListSpace` and `SerializeParamList`.
fn typlenbyval_or_assumed(type_oid: Oid) -> PgResult<(i16, bool)> {
    if oid_is_valid(type_oid) {
        lsyscache_seam::get_typlenbyval::call(type_oid)
    } else {
        // C: `{ sizeof(Datum), true }` — `sizeof(Datum)` is the machine word.
        Ok((core::mem::size_of::<usize>() as i16, true))
    }
}

/// `add_size(s1, s2)` (`storage/shmem.c`): checked size addition raising the C
/// "requested shared memory size overflows size_t" error on overflow.
fn add_size(s1: usize, s2: usize) -> PgResult<usize> {
    s1.checked_add(s2)
        .ok_or_else(|| PgError::error("requested shared memory size overflows size_t"))
}

// ===========================================================================
// SerializeParamList
// ===========================================================================

/// `SerializeParamList(paramLI, &start_address)` (params.c): serialize a
/// `ParamListInfo` into caller-provided storage, advancing and returning the
/// cursor. The number of parameters is written first as a 4-byte integer, then
/// each parameter as a 4-byte type OID, 2 bytes of flags, and the datum as
/// produced by `datumSerialize`. `paramValuesStr` is not included.
///
/// `start_address` mirrors C's `char **start_address`; the returned pointer is
/// the C `*start_address` after the writes. The caller is responsible for having
/// sized the buffer via [`EstimateParamListSpace`].
///
/// # Safety
///
/// `start_address` must point into a writable buffer with at least
/// `EstimateParamListSpace(paramLI)` bytes remaining.
pub unsafe fn SerializeParamList(
    param_li: ParamListInfoHandle,
    start_address: *mut u8,
) -> *mut u8 {
    let mut cursor = start_address;

    // Write number of parameters.
    let live = param_li != ParamListInfoHandle::NULL && {
        with_list(param_li, |pl| pl.num_params > 0)
    };
    let snapshot = if live { Some(snapshot(param_li)) } else { None };
    let nparams: i32 = snapshot.as_ref().map_or(0, |pl| pl.num_params);

    // memcpy(*start_address, &nparams, sizeof(int)); *start_address += sizeof(int);
    core::ptr::copy_nonoverlapping(
        nparams.to_ne_bytes().as_ptr(),
        cursor,
        SIZEOF_INT,
    );
    cursor = cursor.add(SIZEOF_INT);

    // Write each parameter in turn.
    if let Some(param_li) = snapshot {
        for i in 0..nparams as usize {
            // give hook a chance in case parameter is dynamic
            let prm = if param_li.param_fetch {
                paramFetch(&param_li, (i + 1) as i32)
            } else {
                param_li.params[i].clone()
            };
            let type_oid = prm.ptype;

            // Write type OID.
            core::ptr::copy_nonoverlapping(type_oid.to_ne_bytes().as_ptr(), cursor, SIZEOF_OID);
            cursor = cursor.add(SIZEOF_OID);

            // Write flags.
            core::ptr::copy_nonoverlapping(
                prm.pflags.to_ne_bytes().as_ptr(),
                cursor,
                SIZEOF_UINT16,
            );
            cursor = cursor.add(SIZEOF_UINT16);

            // Write datum/isnull.
            let (typ_len, typ_byval) = match typlenbyval_or_assumed(type_oid) {
                Ok(t) => t,
                // get_typlenbyval ereports; SerializeParamList is `void` in C and
                // the failure aborts the (de)serialize. Surface as a panic since
                // the unsafe cursor contract has no PgResult channel.
                Err(e) => panic!("SerializeParamList: get_typlenbyval failed: {e:?}"),
            };
            cursor = datum_seam::datum_serialize_v::call(
                &prm.value,
                prm.isnull,
                typ_byval,
                typ_len as i32,
                cursor,
            );
        }
    }

    cursor
}

// ===========================================================================
// RestoreParamList
// ===========================================================================

/// `RestoreParamList(&start_address)` (params.c): recreate a static,
/// self-contained `ParamListInfo` from the serialized representation, advancing
/// the cursor. The result is what [`copyParamList`] would create. Returns the
/// new handle and the advanced cursor.
///
/// # Safety
///
/// `start_address` must point into a buffer produced by [`SerializeParamList`]
/// with the full serialized image remaining.
pub unsafe fn RestoreParamList(
    start_address: *mut u8,
) -> PgResult<(ParamListInfoHandle, *mut u8)> {
    let mut cursor = start_address;

    // memcpy(&nparams, *start_address, sizeof(int)); *start_address += sizeof(int);
    let mut nbuf = [0u8; SIZEOF_INT];
    core::ptr::copy_nonoverlapping(cursor, nbuf.as_mut_ptr(), SIZEOF_INT);
    let nparams = i32::from_ne_bytes(nbuf);
    cursor = cursor.add(SIZEOF_INT);

    let param_li = makeParamList(nparams)?;

    for i in 0..nparams.max(0) as usize {
        // Read type OID.
        let mut obuf = [0u8; SIZEOF_OID];
        core::ptr::copy_nonoverlapping(cursor, obuf.as_mut_ptr(), SIZEOF_OID);
        let ptype = u32::from_ne_bytes(obuf);
        cursor = cursor.add(SIZEOF_OID);

        // Read flags.
        let mut fbuf = [0u8; SIZEOF_UINT16];
        core::ptr::copy_nonoverlapping(cursor, fbuf.as_mut_ptr(), SIZEOF_UINT16);
        let pflags = u16::from_ne_bytes(fbuf);
        cursor = cursor.add(SIZEOF_UINT16);

        // Read datum/isnull. `datum_restore` is the transitional bare-word seam
        // (no `*_v` form exists yet); wrap its scalar word into the canonical
        // by-value arm. By-reference values restored over a byte image are
        // produced by the owner's seam impl as a `ByVal` carrying a pointer-word
        // under the transitional model — preserved verbatim here.
        let (value, isnull, adv) = datum_seam::datum_restore::call(cursor);
        cursor = adv;

        with_list_mut(param_li, |pl| {
            let prm = &mut pl.params[i];
            prm.ptype = ptype;
            prm.pflags = pflags;
            prm.value = Datum::ByVal(value.as_usize());
            prm.isnull = isnull;
        });
    }

    Ok((param_li, cursor))
}

// ===========================================================================
// BuildParamLogString
// ===========================================================================

/// `BuildParamLogString(params, knownTextValues, maxlen)` (params.c): build a
/// string representation of the parameter list, for logging. Returns `None` when
/// a param fetch hook is in use or in an aborted transaction (C's `return
/// NULL`).
///
/// If the caller already knows textual representations for some parameters it
/// passes `known_text_values`, a slice of exactly `params.numParams` entries
/// (any of which may be `None`); an empty slice means none are known. If
/// `maxlen >= 0`, that is the max number of bytes of any one parameter value to
/// be printed, with an ellipsis added when truncated.
///
/// C uses a temporary memory context for the output-function calls; the owned
/// model takes the caller's `mcx` for the `OidOutputFunctionCall` result bytes.
pub fn BuildParamLogString<'mcx>(
    mcx: Mcx<'mcx>,
    params: ParamListInfoHandle,
    known_text_values: &[Option<String>],
    maxlen: i32,
) -> PgResult<Option<String>> {
    // NB: think not of returning params->paramValuesStr; it may have been
    // generated with a different maxlen, and this is what creates that string.

    // No work if the param fetch hook is in use, nor in an aborted transaction.
    let params = snapshot(params);
    if params.param_fetch || xact_seam::is_aborted_transaction_block_state::call() {
        return Ok(None);
    }

    let mut buf = String::new();

    for paramno in 0..params.num_params as usize {
        let param = &params.params[paramno];

        // appendStringInfo(&buf, "%s$%d = ", paramno > 0 ? ", " : "", paramno + 1)
        if paramno > 0 {
            buf.push_str(", ");
        }
        use core::fmt::Write;
        let _ = write!(buf, "${} = ", paramno + 1);

        if param.isnull || !oid_is_valid(param.ptype) {
            buf.push_str("NULL");
        } else if let Some(Some(known)) = known_text_values.get(paramno) {
            append_string_info_string_quoted(&mut buf, known, maxlen);
        } else {
            let (typoutput, _typisvarlena) =
                lsyscache_seam::get_type_output_info::call(param.ptype)?;
            let pstring =
                fmgr_seam::oid_output_function_call::call(mcx, typoutput, &param.value)?;
            let s = core::str::from_utf8(&pstring)
                .expect("type output function returns valid UTF-8 text");
            append_string_info_string_quoted(&mut buf, s, maxlen);
        }
    }

    Ok(Some(buf))
}

/// `appendStringInfoStringQuoted(str, s, maxlen)` (`mb/stringinfo_mb.c`): append
/// up to `maxlen` bytes of `s` (or all of it when `maxlen < 0`), wrapped in
/// single quotes with every embedded `'` doubled, adding `...` before the
/// closing quote when the copy is truncated.
///
/// C clips with `pg_mbcliplen` (never splitting a multibyte char); here the clip
/// is a byte clip rounded down to a UTF-8 char boundary — identical for the
/// ASCII output-function strings this is used on and never invalid UTF-8.
fn append_string_info_string_quoted(buf: &mut String, s: &str, maxlen: i32) {
    let slen = s.len();
    let (clipped, ellipsis) = if maxlen >= 0 && (maxlen as usize) < slen {
        let mut finallen = maxlen as usize;
        while finallen > 0 && !s.is_char_boundary(finallen) {
            finallen -= 1;
        }
        (&s[..finallen], true)
    } else {
        (s, false)
    };

    buf.push('\'');
    for ch in clipped.chars() {
        if ch == '\'' {
            buf.push('\'');
        }
        buf.push(ch);
    }
    if ellipsis {
        buf.push_str("...");
    }
    buf.push('\'');
}

// ===========================================================================
// ParamsErrorCallback
// ===========================================================================

/// `ParamsErrorCallback(arg)` (params.c): error-context callback computing the
/// "portal ... with parameters: ..." context line. A no-op (returns `None`)
/// unless [`BuildParamLogString`] populated `paramValuesStr` beforehand.
///
/// C appends to the in-progress error via `errcontext()` (untranslated). The
/// owned model returns the computed context string (or `None`) for the caller to
/// splice in; the text matches C byte for byte.
pub fn ParamsErrorCallback(data: Option<&ParamsErrorCbData>) -> Option<String> {
    let data = data?;
    let params = data.params.as_ref()?;
    let param_values = params.param_values_str.as_ref()?;

    match &data.portal_name {
        Some(name) if !name.is_empty() => Some(format!(
            "portal \"{name}\" with parameters: {param_values}"
        )),
        _ => Some(format!("unnamed portal with parameters: {param_values}")),
    }
}
