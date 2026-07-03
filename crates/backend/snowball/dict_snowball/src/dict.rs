use core::ffi::c_int;

use ::mcx::{Mcx, PgVec};
use ::ts_locale::dict_api::{DictInitData, LexizeResult};
use ::ts_locale::{lowerstr, readstoplist, searchstoplist, StopList, TsLexeme};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_OBJECT};
use ::wchar::{pg_enc, PG_LATIN1, PG_SQL_ASCII, PG_UTF8};

use crate::api::SN_set_current;
use crate::types::SN_env;

struct StemmerModule {
    name: &'static str,
    enc: pg_enc,
    create: unsafe fn() -> *mut SN_env,
    stem: unsafe fn(*mut SN_env) -> c_int,
}

static STEMMER_MODULES: [StemmerModule; 3] = [
    StemmerModule {
        name: "english",
        enc: PG_LATIN1,
        create: crate::stemmers::stem_iso_8859_1_english::english_ISO_8859_1_create_env,
        stem: crate::stemmers::stem_iso_8859_1_english::english_ISO_8859_1_stem,
    },
    StemmerModule {
        name: "english",
        enc: PG_UTF8,
        create: crate::stemmers::stem_utf8_english::english_UTF_8_create_env,
        stem: crate::stemmers::stem_utf8_english::english_UTF_8_stem,
    },
    StemmerModule {
        name: "english",
        enc: PG_SQL_ASCII,
        create: crate::stemmers::stem_iso_8859_1_english::english_ISO_8859_1_create_env,
        stem: crate::stemmers::stem_iso_8859_1_english::english_ISO_8859_1_stem,
    },
];

// dict_snowball.c stemmer_modules languages whose automatons are unported;
// requesting one is a loud gap, not C's "no stemmer available" error.
static UNPORTED_LANGS: &[&str] = &[
    "arabic",
    "armenian",
    "basque",
    "catalan",
    "danish",
    "dutch",
    "estonian",
    "finnish",
    "french",
    "german",
    "greek",
    "hindi",
    "hungarian",
    "indonesian",
    "irish",
    "italian",
    "lithuanian",
    "nepali",
    "norwegian",
    "porter",
    "portuguese",
    "romanian",
    "russian",
    "serbian",
    "spanish",
    "swedish",
    "tamil",
    "turkish",
    "yiddish",
];

pub struct DictSnowball {
    z: *mut SN_env,
    stem: unsafe fn(*mut SN_env) -> c_int,
    stoplist: StopList<'static>,
    needrecode: bool,
}

fn eq_strcasecmp(a: &str, b: &[u8]) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b)
            .all(|(x, &y)| x.to_ascii_lowercase() == y.to_ascii_lowercase())
}

struct Located {
    z: *mut SN_env,
    stem: unsafe fn(*mut SN_env) -> c_int,
    needrecode: bool,
}

fn locate_stem_module(lang: &[u8]) -> PgResult<Located> {
    let db_enc = ::mbutils::GetDatabaseEncoding();
    for m in &STEMMER_MODULES {
        if (m.enc == PG_SQL_ASCII || m.enc == db_enc) && eq_strcasecmp(m.name, lang) {
            // SAFETY: generated stemmer constructor allocating via crate::mem.
            let z = unsafe { (m.create)() };
            return Ok(Located { z, stem: m.stem, needrecode: false });
        }
    }
    for m in &STEMMER_MODULES {
        if m.enc == PG_UTF8 && eq_strcasecmp(m.name, lang) {
            // SAFETY: as above.
            let z = unsafe { (m.create)() };
            return Ok(Located { z, stem: m.stem, needrecode: true });
        }
    }
    if UNPORTED_LANGS
        .iter()
        .any(|l| eq_strcasecmp(l, lang))
    {
        panic!(
            "dict_snowball: stemmer \"{}\" unported (tsearch-lane: english only)",
            String::from_utf8_lossy(lang)
        );
    }
    Err(PgError::error(format!(
        "no Snowball stemmer available for language \"{}\" and encoding \"{}\"",
        String::from_utf8_lossy(lang),
        ::mbutils::GetDatabaseEncodingName()
    ))
    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
    .into())
}

fn invalid_param(msg: &str) -> PgError {
    PgError::error(msg.to_string()).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

pub fn dsnowball_init(init: &DictInitData<'static>) -> PgResult<DictSnowball> {
    let mcx = init.mcx;
    let mut located: Option<Located> = None;
    let mut stoplist: Option<StopList<'static>> = None;

    for (name, value) in &init.dict_options {
        if name.as_slice() == b"stopwords" {
            if stoplist.is_some() {
                return Err(invalid_param("multiple StopWords parameters").into());
            }
            stoplist = Some(readstoplist(mcx, Some(value.as_slice()), true)?);
        } else if name.as_slice() == b"language" {
            if located.is_some() {
                return Err(invalid_param("multiple Language parameters").into());
            }
            located = Some(locate_stem_module(value.as_slice())?);
        } else {
            return Err(invalid_param(&format!(
                "unrecognized Snowball parameter: \"{}\"",
                String::from_utf8_lossy(name)
            ))
            .into());
        }
    }

    let Some(located) = located else {
        return Err(invalid_param("missing Language parameter").into());
    };

    Ok(DictSnowball {
        z: located.z,
        stem: located.stem,
        stoplist: stoplist.unwrap_or(StopList { stop: PgVec::new_in(mcx) }),
        needrecode: located.needrecode,
    })
}

pub fn dsnowball_lexize<'mcx>(
    mcx: Mcx<'mcx>,
    d: &DictSnowball,
    token: &[u8],
) -> PgResult<LexizeResult<'mcx>> {
    let mut txt: PgVec<'mcx, u8> = lowerstr(mcx, token)?;

    if token.len() > 1000 {
        return one_lexeme(mcx, txt);
    }
    if txt.is_empty() || searchstoplist(&d.stoplist, &txt) {
        return Ok(LexizeResult(PgVec::new_in(mcx)));
    }

    if d.needrecode {
        if let Some(recoded) = ::mbutils::pg_server_to_any(mcx, &txt, PG_UTF8)? {
            txt = recoded;
        }
    }

    // SAFETY: d.z is this dictionary's live SN_env; the stemmer touches only
    // the env and its own buffers.
    let (out_p, out_l) = unsafe {
        SN_set_current(d.z, txt.len() as c_int, txt.as_ptr());
        (d.stem)(d.z);
        ((*d.z).p, (*d.z).l)
    };
    if !out_p.is_null() && out_l != 0 {
        let n = out_l as usize;
        let mut stemmed: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        // SAFETY: the stemmer leaves z->l valid bytes at z->p.
        ::mcx::vec_append_bytes(&mut stemmed, unsafe { core::slice::from_raw_parts(out_p, n) })?;
        txt = stemmed;
    }

    if d.needrecode {
        if let Some(recoded) = ::mbutils::pg_any_to_server(mcx, &txt, PG_UTF8)? {
            txt = recoded;
        }
    }

    one_lexeme(mcx, txt)
}

fn one_lexeme<'mcx>(mcx: Mcx<'mcx>, lexeme: PgVec<'mcx, u8>) -> PgResult<LexizeResult<'mcx>> {
    let mut out: PgVec<'mcx, TsLexeme<'mcx>> = PgVec::new_in(mcx);
    out.try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<TsLexeme>()))?;
    out.push(TsLexeme { nvariant: 0, flags: 0, lexeme });
    Ok(LexizeResult(out))
}
