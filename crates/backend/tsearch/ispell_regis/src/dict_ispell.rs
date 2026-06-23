//! Port of `src/backend/tsearch/dict_ispell.c` — the `ispell` dictionary
//! template.
//!
//! On init it imports the `DictFile`/`AffFile` and loads an optional
//! `StopWords` list (building an [`IspellDict`] behind the spell unit's
//! [`SpellHandle`](tsearch::SpellHandle)); on lexize it lowercases the
//! token, normalizes it via `NINormalizeWord`, and drops stop words.
//!
//! The `IspellDict` build pipeline (`NIStartBuild` .. `NIFinishBuild`,
//! `NINormalizeWord`) belongs to the unported `spell.c`, reached through
//! `backend-tsearch-spell-seams`; the stop list and config-file helpers belong
//! to the unported `ts_utils.c`, reached through
//! `backend-tsearch-ts-utils-seams`; `str_tolower` and `defGetString` cross to
//! their owners too.

use define_seams::{def_get_string, DefElemArg};
use spell_seams::{
    spell_finish_build, spell_import_affixes, spell_import_dictionary, spell_normalize_word,
    spell_sort_affixes, spell_sort_dictionary, spell_start_build,
};
use ts_utils_seams::{get_tsearch_config_filename, readstoplist, searchstoplist};
use formatting_seams::str_tolower;
use alloc::string::String;

use utils_error::ereport;
use mcx::{Mcx, PgVec};
use types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use tsearch::{DictISpell, StopList, TSLexeme};

/// `DEFAULT_COLLATION_OID` (`pg_collation_d.h`).
const DEFAULT_COLLATION_OID: types_core::Oid = 100;

/// `dispell_init(PG_FUNCTION_ARGS)`: parse `DictFile`/`AffFile`/`StopWords`,
/// then run the ISpell build pipeline (`NIStartBuild` .. `NIFinishBuild`).
///
/// `dictoptions` is the C `List *` of `DefElem`s, each `(defname, def->arg)`.
/// The built [`DictISpell`] is allocated in `mcx`.
pub fn dispell_init<'mcx>(
    mcx: Mcx<'mcx>,
    dictoptions: &[(String, Option<DefElemArg>)],
) -> PgResult<DictISpell<'mcx>> {
    let mut affloaded = false;
    let mut dictloaded = false;
    let mut stoploaded = false;

    // C: d = palloc0(sizeof(DictISpell)); the stop list starts empty.
    let mut stoplist = StopList {
        stop: PgVec::new_in(mcx),
    };

    // NIStartBuild(&(d->obj));
    let obj = spell_start_build::call()?;

    for (defname, arg) in dictoptions {
        if defname == "dictfile" {
            if dictloaded {
                return Err(invalid_param("multiple DictFile parameters"));
            }
            let base = def_get_string::call(mcx, defname.clone(), arg.clone())?;
            let path = get_tsearch_config_filename::call(mcx, base.as_bytes(), b"dict")?;
            spell_import_dictionary::call(obj, &path)?;
            dictloaded = true;
        } else if defname == "afffile" {
            if affloaded {
                return Err(invalid_param("multiple AffFile parameters"));
            }
            let base = def_get_string::call(mcx, defname.clone(), arg.clone())?;
            let path = get_tsearch_config_filename::call(mcx, base.as_bytes(), b"affix")?;
            spell_import_affixes::call(obj, &path)?;
            affloaded = true;
        } else if defname == "stopwords" {
            if stoploaded {
                return Err(invalid_param("multiple StopWords parameters"));
            }
            let base = def_get_string::call(mcx, defname.clone(), arg.clone())?;
            // C: readstoplist(defGetString(defel), &(d->stoplist), str_tolower);
            stoplist = readstoplist::call(mcx, base.as_bytes(), true)?;
            stoploaded = true;
        } else {
            return Err(invalid_param(alloc::format!(
                "unrecognized Ispell parameter: \"{defname}\""
            )));
        }
    }

    if affloaded && dictloaded {
        spell_sort_dictionary::call(obj)?;
        spell_sort_affixes::call(obj)?;
    } else if !affloaded {
        return Err(invalid_param("missing AffFile parameter"));
    } else {
        return Err(invalid_param("missing DictFile parameter"));
    }

    // NIFinishBuild(&(d->obj));
    spell_finish_build::call(obj)?;

    Ok(DictISpell { stoplist, obj })
}

/// `dispell_lexize(PG_FUNCTION_ARGS)`: lowercase, normalize, drop stop words.
/// Returns `None` for the C `PG_RETURN_POINTER(NULL)`.
///
/// `input`/`len` are the C `char *in` / `int32 len` lexize arguments. The kept
/// lexemes are allocated in `mcx`.
pub fn dispell_lexize<'mcx>(
    mcx: Mcx<'mcx>,
    d: &DictISpell<'_>,
    input: &[u8],
    len: i32,
) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>> {
    if len <= 0 {
        return Ok(None);
    }

    // C: txt = str_tolower(in, len, DEFAULT_COLLATION_OID).
    let in_bytes = &input[..len as usize];
    let txt = str_tolower::call(mcx, in_bytes, DEFAULT_COLLATION_OID)?;

    // res = NINormalizeWord(&(d->obj), txt);
    let res = spell_normalize_word::call(mcx, d.obj, &txt)?;

    let Some(res) = res else {
        return Ok(None);
    };

    // Compact the result, dropping stop-word lexemes (the C cptr<=ptr walk:
    //   if (searchstoplist(&d->stoplist, ptr->lexeme)) { pfree; lexeme=NULL; }
    //   else { if (cptr != ptr) memcpy(cptr, ptr); cptr++; }
    //   cptr->lexeme = NULL;
    // — an in-place retain on the lexeme array).
    let mut kept: PgVec<'mcx, TSLexeme<'mcx>> = PgVec::new_in(mcx);
    for lex in res {
        if searchstoplist::call(&d.stoplist, lex.lexeme.as_bytes()) {
            // C: pfree(ptr->lexeme); ptr->lexeme = NULL; (dropped by not keeping).
            continue;
        }
        kept.try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<TSLexeme>()))?;
        kept.push(lex);
    }

    Ok(Some(kept))
}

/// An `ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE, errmsg(...))` for the
/// ispell-option diagnostics.
fn invalid_param(message: impl Into<alloc::string::String>) -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(message)
        .into_error()
}
