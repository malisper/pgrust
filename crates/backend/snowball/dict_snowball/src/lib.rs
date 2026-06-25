//! Port of `src/backend/snowball/dict_snowball.c` — the Snowball-stemmer
//! text-search dictionary template.
//!
//! `dsnowball_init` parses the `Language`/`StopWords` options, locates the
//! per-language Snowball stemmer module for the database encoding (the
//! `stemmer_modules[]` table, here [`::runtime::STEMMER_MODULES`]),
//! creates a live `SN_env`, and loads the optional stop list. `dsnowball_lexize`
//! lowercases the token, drops stop words, then runs the stemmer (recoding
//! to/from UTF-8 when the matched UTF-8 stemmer's encoding differs from the
//! server encoding), returning the single stemmed lexeme.
//!
//! The per-language stemmer automatons and the libstemmer runtime
//! (`SN_create_env`/`SN_set_current`/`find_among`/…) live in the
//! `backend-snowball-runtime` crate; the live `SN_env *` and stem fn pointer
//! cross the type/backend layering boundary behind
//! [`::tsearch::SnowballEnvHandle`], resolved through this crate's
//! [`env_registry`]. Stop-list/config-file helpers cross to the (unported)
//! `ts_utils.c` through `backend-tsearch-ts-utils-seams`; `str_tolower` to
//! `formatting.c`; `GetDatabaseEncoding`/`pg_server_to_any`/`pg_any_to_server`
//! to `mbutils.c`; `defGetString` to `define.c` — each through its owner seam
//! crate.

extern crate alloc;

use alloc::string::String;
use core::cell::RefCell;
use core::ffi::c_int;

use ::define_seams::{def_get_string, DefElemArg};
use ::runtime::{SN_env, SN_set_current, STEMMER_MODULES, PG_SQL_ASCII, PG_UTF8};
use ::ts_utils_seams::{readstoplist, searchstoplist};
use ::formatting_seams::str_tolower;
use ::utils_error::ereport;
use ::mbutils_seams::{
    get_database_encoding, get_database_encoding_name, pg_any_to_server, pg_server_to_any,
};
use ::mcx::{Mcx, PgString, PgVec};
use ::types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use ::tsearch::{DictSnowball, SnowballEnvHandle, StopList, TSLexeme};
use ::datum::Datum;
use ::fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};

pub mod mem_provider;

/// The simple library name the `snowball` template's SQL functions reference
/// (`AS '$libdir/dict_snowball', 'dsnowball_init'`). Registered with the dfmgr
/// builtin-library registry so `CREATE FUNCTION ... LANGUAGE C` resolves it
/// without touching the OS loader (there is no `dict_snowball.so`).
const LIBRARY: &str = "dict_snowball";

/// `DEFAULT_COLLATION_OID` (`pg_collation_d.h`).
const DEFAULT_COLLATION_OID: types_core::Oid = 100;

// ===========================================================================
// SN_env registry — resolves the opaque `SnowballEnvHandle` to the live
// `*mut SN_env` + stem fn the C `DictSnowball` holds inline.
//
// C stores `struct SN_env *z` and `int (*stem)(struct SN_env *)` directly in
// the palloc'd `DictSnowball`. Those raw runtime pointers cannot be named in
// the `types-tsearch` layer (it must not depend on a backend crate), so the
// value-typed `DictSnowball` carries a `SnowballEnvHandle` token; this side
// table — owned by the dict unit — maps the token to the real pointers, the
// same shape the spell unit uses for its `IspellDict`.
// ===========================================================================
pub mod env_registry {
    use super::*;

    /// The live stemmer environment + its stem method (the C `z` + `stem`).
    #[derive(Copy, Clone)]
    pub struct StemEnv {
        pub z: *mut SN_env,
        pub stem: unsafe fn(*mut SN_env) -> c_int,
    }

    thread_local! {
        static REG: RefCell<alloc::vec::Vec<StemEnv>> = const { RefCell::new(alloc::vec::Vec::new()) };
    }

    /// Register a live environment, returning its handle.
    pub fn register(env: StemEnv) -> SnowballEnvHandle {
        REG.with(|r| {
            let mut v = r.borrow_mut();
            v.push(env);
            SnowballEnvHandle(v.len() as u64) // 1-based; 0 is never a valid handle
        })
    }

    /// Resolve a handle to its live environment.
    pub fn resolve(h: SnowballEnvHandle) -> StemEnv {
        REG.with(|r| {
            let v = r.borrow();
            *v.get((h.0 as usize).wrapping_sub(1))
                .expect("backend-snowball-dict-snowball: unknown SnowballEnvHandle")
        })
    }
}

/// `pg_strcasecmp(s1, s2) == 0` over NUL-free byte strings (the stemmer module
/// names and the requested language are ASCII). `src/port/pgstrcasecmp.c`.
fn strcasecmp_eq(s1: &[u8], s2: &[u8]) -> bool {
    if s1.len() != s2.len() {
        return false;
    }
    s1.iter().zip(s2).all(|(&a, &b)| {
        let la = if a.is_ascii_uppercase() { a + 32 } else { a };
        let lb = if b.is_ascii_uppercase() { b + 32 } else { b };
        la == lb
    })
}

/// An `ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE, errmsg(...))`.
fn invalid_param(message: impl Into<String>) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(message)
        .into_error()
}

/// `locate_stem_module(d, lang)` (dict_snowball.c): find and instantiate the
/// stemmer module matching `lang` for the database encoding. Sets `d.z` (a new
/// handle) and `d.needrecode`. Raises `ERRCODE_UNDEFINED_OBJECT` if none match.
fn locate_stem_module(d: &mut DictSnowball<'_>, lang: &[u8]) -> PgResult<()> {
    let db_enc = get_database_encoding::call();

    // First, try an exact encoding match (PG_SQL_ASCII works for any encoding).
    for m in STEMMER_MODULES.iter() {
        if (m.enc == PG_SQL_ASCII || m.enc == db_enc) && strcasecmp_eq(m.name.as_bytes(), lang) {
            // SAFETY: the create fn is a faithful generated stemmer constructor;
            // it allocates through the runtime mem seam.
            let z = unsafe { (m.create)() };
            d.z = Some(env_registry::register(env_registry::StemEnv { z, stem: m.stem }));
            d.needrecode = false;
            return Ok(());
        }
    }

    // Second, try a UTF-8 stemmer for the language (with recoding).
    for m in STEMMER_MODULES.iter() {
        if m.enc == PG_UTF8 && strcasecmp_eq(m.name.as_bytes(), lang) {
            // SAFETY: see above.
            let z = unsafe { (m.create)() };
            d.z = Some(env_registry::register(env_registry::StemEnv { z, stem: m.stem }));
            d.needrecode = true;
            return Ok(());
        }
    }

    let lang_str = String::from_utf8_lossy(lang);
    let enc_name = get_database_encoding_name::call();
    Err(ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_OBJECT)
        .errmsg(alloc::format!(
            "no Snowball stemmer available for language \"{lang_str}\" and encoding \"{enc_name}\""
        ))
        .into_error())
}

/// `dsnowball_init(PG_FUNCTION_ARGS)`: parse `Language`/`StopWords`, locate the
/// stemmer module, load the optional stop list. The built [`DictSnowball`] is
/// allocated in `mcx`.
///
/// `dictoptions` is the C `List *` of `DefElem`s, each `(defname, def->arg)`.
pub fn dsnowball_init<'mcx>(
    mcx: Mcx<'mcx>,
    dictoptions: &[(String, Option<DefElemArg>)],
) -> PgResult<DictSnowball<'mcx>> {
    // C: d = palloc0(sizeof(DictSnowball)); stem/z NULL, stoplist empty.
    let mut d = DictSnowball {
        z: None,
        stoplist: StopList {
            stop: PgVec::new_in(mcx),
        },
        needrecode: false,
    };
    let mut stoploaded = false;

    for (defname, arg) in dictoptions {
        if defname == "stopwords" {
            if stoploaded {
                return Err(invalid_param("multiple StopWords parameters"));
            }
            // C: readstoplist(defGetString(defel), &d->stoplist, str_tolower);
            let base = def_get_string::call(mcx, defname.clone(), arg.clone())?;
            d.stoplist = readstoplist::call(mcx, base.as_bytes(), true)?;
            stoploaded = true;
        } else if defname == "language" {
            if d.z.is_some() {
                return Err(invalid_param("multiple Language parameters"));
            }
            let base = def_get_string::call(mcx, defname.clone(), arg.clone())?;
            locate_stem_module(&mut d, base.as_bytes())?;
        } else {
            return Err(invalid_param(alloc::format!(
                "unrecognized Snowball parameter: \"{defname}\""
            )));
        }
    }

    if d.z.is_none() {
        return Err(invalid_param("missing Language parameter"));
    }

    // C: d->dictCtx = CurrentMemoryContext; the dictionary's allocations (the
    // SN_env buffers) live in the long-lived dictionary cache context, which is
    // `mcx` here (the C remembers it solely to switch into it around the stem
    // call, which the runtime mem seam handles).
    Ok(d)
}

/// `dsnowball_lexize(PG_FUNCTION_ARGS)`: lowercase, drop stop words, stem.
/// Returns the single stemmed lexeme; `None` mirrors the C lexeme staying NULL
/// (empty or stop-word token). The kept `TSLexeme` is allocated in `mcx`.
///
/// `input`/`len` are the C `char *in` / `int32 len` lexize arguments.
pub fn dsnowball_lexize<'mcx>(
    mcx: Mcx<'mcx>,
    d: &DictSnowball<'_>,
    input: &[u8],
    len: i32,
) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>> {
    // C: txt = str_tolower(in, len, DEFAULT_COLLATION_OID).
    let in_bytes = &input[..(len.max(0) as usize).min(input.len())];
    let mut txt: PgVec<'mcx, u8> = str_tolower::call(mcx, in_bytes, DEFAULT_COLLATION_OID)?;

    if len > 1000 {
        // C: return the lexeme lowercased, but otherwise unmodified.
        return Ok(Some(one_lexeme(mcx, txt)?));
    }

    // C: *txt == '\0' || searchstoplist(&d->stoplist, txt) → report as stopword.
    // C ALWAYS returns the `res = palloc0(2 * TSLexeme)` array (never NULL); a
    // stopword leaves `res[0].lexeme == NULL`, i.e. an EMPTY but non-NULL array.
    // That distinction is load-bearing: in `parsetext`/`LexizeExec` a non-NULL
    // empty array means "stopword — consume a position", whereas a NULL means
    // "dictionary doesn't recognize the word — try the next dictionary".
    // Snowball recognizes every string, so it must never return NULL here.
    if txt.is_empty() || searchstoplist::call(&d.stoplist, &txt) {
        return Ok(Some(PgVec::new_in(mcx)));
    }

    // C: recode to UTF-8 if the stemmer is UTF-8 and the server encoding differs.
    if d.needrecode {
        if let Some(recoded) = pg_server_to_any::call(mcx, &txt, PG_UTF8)? {
            txt = recoded;
        }
    }

    // C: switch to dictCtx; SN_set_current(z, len, txt); d->stem(z); switch back.
    let env = env_registry::resolve(
        d.z.expect("dsnowball_lexize: lexize called on a dictionary with no stemmer"),
    );
    // SAFETY: `env.z` is a live SN_env from this dictionary's create fn; the
    // stemmer reads/writes only that env and the working buffer it owns.
    let (out_p, out_l) = unsafe {
        SN_set_current(env.z, txt.len() as c_int, txt.as_ptr());
        (env.stem)(env.z);
        ((*env.z).p, (*env.z).l)
    };

    // C: if (d->z->p && d->z->l) { copy z->p[0..z->l] into txt }.
    if !out_p.is_null() && out_l != 0 {
        let n = out_l as usize;
        let mut stemmed: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        stemmed
            .try_reserve(n)
            .map_err(|_| mcx.oom(n))?;
        // SAFETY: the stemmer leaves `z->l` valid bytes at `z->p`.
        for i in 0..n {
            stemmed.push(unsafe { *out_p.add(i) });
        }
        txt = stemmed;
    }

    // C: back-recode if needed.
    if d.needrecode {
        if let Some(recoded) = pg_any_to_server::call(mcx, &txt, PG_UTF8)? {
            txt = recoded;
        }
    }

    Ok(Some(one_lexeme(mcx, txt)?))
}

/// Build the C `res` array: `palloc0(sizeof(TSLexeme)*2)` with `res->lexeme`
/// set and a trailing NULL-lexeme terminator (here, the end of the [`PgVec`]).
///
/// `bytes` is the final server-encoded lexeme text; the owned model carries
/// lexemes as UTF-8 [`PgString`]s, so the (already lowercased / server-encoded)
/// bytes are wrapped as UTF-8 — reusing the allocation when valid, copying via
/// the lossy form otherwise.
fn one_lexeme<'mcx>(
    mcx: Mcx<'mcx>,
    bytes: PgVec<'mcx, u8>,
) -> PgResult<PgVec<'mcx, TSLexeme<'mcx>>> {
    let lexeme = if core::str::from_utf8(&bytes).is_ok() {
        // Valid UTF-8: reuse the allocation.
        PgString::from_utf8(bytes).expect("validated above")
    } else {
        // Non-UTF-8 server encoding: copy via the lossy form (model limit:
        // TSLexeme carries UTF-8 text).
        PgString::from_str_in(&String::from_utf8_lossy(&bytes), mcx)?
    };
    let mut out: PgVec<'mcx, TSLexeme<'mcx>> = PgVec::new_in(mcx);
    out.try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<TSLexeme>()))?;
    out.push(TSLexeme {
        nvariant: 0,
        flags: 0,
        lexeme,
    });
    Ok(out)
}

// ===========================================================================
// Builtin-library registration.
//
// `snowball_create.sql` (run during initdb) creates the `snowball` template's
// underlying C functions:
//   CREATE FUNCTION dsnowball_init(INTERNAL)
//       RETURNS INTERNAL AS '$libdir/dict_snowball', 'dsnowball_init' …
//   CREATE FUNCTION dsnowball_lexize(INTERNAL, …)
//       RETURNS INTERNAL AS '$libdir/dict_snowball', 'dsnowball_lexize' …
// The Rust backend exposes no C ABI, so there is no `dict_snowball.so` to
// `dlopen`; this unit's bodies are ported in-process. Registering them with the
// dfmgr builtin-library registry lets `CREATE FUNCTION … LANGUAGE C` resolve the
// `(library, symbol)` pair (validating the pg_proc rows) instead of erroring
// with "could not access file dict_snowball".
//
// These `PGFunction` entry points are not actually invoked through fmgr at run
// time: the text-search dictionary machinery (`to_tsany`'s lexize dispatch)
// recognizes the `snowball` template by its `INIT`/`LEXIZE` method names and
// calls [`dsnowball_init`] / [`dsnowball_lexize`] directly with the typed
// (`Mcx`-bound) arguments. The wrappers below therefore only ever run if some
// path were to dispatch these INTERNAL-typed functions through the generic fmgr
// machinery (which C never does either — they are dictionary template methods),
// in which case they raise the same error the C bodies would on a bare call.
// ===========================================================================

/// Raise a structured `ereport(ERROR)` through the `PGFunction` dispatch point
/// (`invoke_pgfunction`'s `catch_unwind`), mirroring the contrib libraries.
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

/// `dsnowball_init` fmgr entry — see the module note above; the dictionary
/// machinery calls [`dsnowball_init`] directly, so reaching here means the
/// INTERNAL template method was invoked through generic fmgr, which is not a
/// supported call path (the C function would likewise fault on its INTERNAL
/// argument). Present so `CREATE FUNCTION` can resolve the symbol.
fn fc_dsnowball_init(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("dsnowball_init may only be called as a text search dictionary template method")
            .into_error(),
    )
}

/// `dsnowball_lexize` fmgr entry — see [`fc_dsnowball_init`].
fn fc_dsnowball_lexize(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(
                "dsnowball_lexize may only be called as a text search dictionary template method",
            )
            .into_error(),
    )
}

/// Resolve a `dict_snowball` symbol to its ported `PGFunction`. `None` for an
/// unknown symbol (the C "could not find function in file" error).
fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        "dsnowball_init" => Some(fc_dsnowball_init),
        "dsnowball_lexize" => Some(fc_dsnowball_lexize),
        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        api_version: 1,
    })
}

/// Install the seams this unit owns (`backend-snowball-dict-snowball-seams`):
/// the `snowball` dictionary template's `dsnowball_init` / `dsnowball_lexize`
/// fmgr methods.
pub fn init_seams() {
    dict_snowball_seams::dsnowball_init::set(dsnowball_init);
    dict_snowball_seams::dsnowball_lexize::set(dsnowball_lexize);
    // Register the `dict_snowball` module with the dynamic-loader's ported-
    // library registry so `CREATE FUNCTION … AS '$libdir/dict_snowball'`
    // resolves in-process (there is no `dict_snowball.so` to dlopen).
    dfmgr_seams::register_builtin_library(dfmgr_seams::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
    // Install the raw-address allocator the libstemmer runtime needs (the
    // backend `palloc` redefinition in `snowball/header.h`).
    mem_provider::install_snowball_alloc();
}
