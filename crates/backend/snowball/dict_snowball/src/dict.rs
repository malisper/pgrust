use core::ffi::c_int;

use ::mcx::{Mcx, PgVec};
use ::ts_locale::dict_api::{DictInitData, LexizeResult};
use ::ts_locale::{lowerstr, readstoplist, searchstoplist, StopList, TsLexeme};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_OBJECT};
use ::wchar::{pg_enc, PG_KOI8R, PG_LATIN1, PG_LATIN2, PG_SQL_ASCII, PG_UTF8};

use crate::api::SN_set_current;
use crate::types::SN_env;

struct StemmerModule {
    name: &'static str,
    enc: pg_enc,
    create: unsafe fn() -> *mut SN_env,
    stem: unsafe fn(*mut SN_env) -> c_int,
    close: unsafe fn(*mut SN_env),
}

macro_rules! sm {
    ($name:literal, $enc:expr, $module:ident, $create:ident, $stem:ident, $close:ident) => {
        StemmerModule {
            name: $name,
            enc: $enc,
            create: crate::stemmers::$module::$create,
            stem: crate::stemmers::$module::$stem,
            close: crate::stemmers::$module::$close,
        }
    };
}

// The full dict_snowball.c stemmer_modules table (PostgreSQL 18.3), in the
// same order: single-byte encodings first, then UTF-8, then the SQL_ASCII
// english fallback.
#[rustfmt::skip]
static STEMMER_MODULES: [StemmerModule; 49] = [
    sm!("basque", PG_LATIN1, stem_iso_8859_1_basque, basque_ISO_8859_1_create_env, basque_ISO_8859_1_stem, basque_ISO_8859_1_close_env),
    sm!("catalan", PG_LATIN1, stem_iso_8859_1_catalan, catalan_ISO_8859_1_create_env, catalan_ISO_8859_1_stem, catalan_ISO_8859_1_close_env),
    sm!("danish", PG_LATIN1, stem_iso_8859_1_danish, danish_ISO_8859_1_create_env, danish_ISO_8859_1_stem, danish_ISO_8859_1_close_env),
    sm!("dutch", PG_LATIN1, stem_iso_8859_1_dutch, dutch_ISO_8859_1_create_env, dutch_ISO_8859_1_stem, dutch_ISO_8859_1_close_env),
    sm!("english", PG_LATIN1, stem_iso_8859_1_english, english_ISO_8859_1_create_env, english_ISO_8859_1_stem, english_ISO_8859_1_close_env),
    sm!("finnish", PG_LATIN1, stem_iso_8859_1_finnish, finnish_ISO_8859_1_create_env, finnish_ISO_8859_1_stem, finnish_ISO_8859_1_close_env),
    sm!("french", PG_LATIN1, stem_iso_8859_1_french, french_ISO_8859_1_create_env, french_ISO_8859_1_stem, french_ISO_8859_1_close_env),
    sm!("german", PG_LATIN1, stem_iso_8859_1_german, german_ISO_8859_1_create_env, german_ISO_8859_1_stem, german_ISO_8859_1_close_env),
    sm!("indonesian", PG_LATIN1, stem_iso_8859_1_indonesian, indonesian_ISO_8859_1_create_env, indonesian_ISO_8859_1_stem, indonesian_ISO_8859_1_close_env),
    sm!("irish", PG_LATIN1, stem_iso_8859_1_irish, irish_ISO_8859_1_create_env, irish_ISO_8859_1_stem, irish_ISO_8859_1_close_env),
    sm!("italian", PG_LATIN1, stem_iso_8859_1_italian, italian_ISO_8859_1_create_env, italian_ISO_8859_1_stem, italian_ISO_8859_1_close_env),
    sm!("norwegian", PG_LATIN1, stem_iso_8859_1_norwegian, norwegian_ISO_8859_1_create_env, norwegian_ISO_8859_1_stem, norwegian_ISO_8859_1_close_env),
    sm!("porter", PG_LATIN1, stem_iso_8859_1_porter, porter_ISO_8859_1_create_env, porter_ISO_8859_1_stem, porter_ISO_8859_1_close_env),
    sm!("portuguese", PG_LATIN1, stem_iso_8859_1_portuguese, portuguese_ISO_8859_1_create_env, portuguese_ISO_8859_1_stem, portuguese_ISO_8859_1_close_env),
    sm!("spanish", PG_LATIN1, stem_iso_8859_1_spanish, spanish_ISO_8859_1_create_env, spanish_ISO_8859_1_stem, spanish_ISO_8859_1_close_env),
    sm!("swedish", PG_LATIN1, stem_iso_8859_1_swedish, swedish_ISO_8859_1_create_env, swedish_ISO_8859_1_stem, swedish_ISO_8859_1_close_env),
    sm!("hungarian", PG_LATIN2, stem_iso_8859_2_hungarian, hungarian_ISO_8859_2_create_env, hungarian_ISO_8859_2_stem, hungarian_ISO_8859_2_close_env),
    sm!("russian", PG_KOI8R, stem_koi8_r_russian, russian_KOI8_R_create_env, russian_KOI8_R_stem, russian_KOI8_R_close_env),
    sm!("arabic", PG_UTF8, stem_utf8_arabic, arabic_UTF_8_create_env, arabic_UTF_8_stem, arabic_UTF_8_close_env),
    sm!("armenian", PG_UTF8, stem_utf8_armenian, armenian_UTF_8_create_env, armenian_UTF_8_stem, armenian_UTF_8_close_env),
    sm!("basque", PG_UTF8, stem_utf8_basque, basque_UTF_8_create_env, basque_UTF_8_stem, basque_UTF_8_close_env),
    sm!("catalan", PG_UTF8, stem_utf8_catalan, catalan_UTF_8_create_env, catalan_UTF_8_stem, catalan_UTF_8_close_env),
    sm!("danish", PG_UTF8, stem_utf8_danish, danish_UTF_8_create_env, danish_UTF_8_stem, danish_UTF_8_close_env),
    sm!("dutch", PG_UTF8, stem_utf8_dutch, dutch_UTF_8_create_env, dutch_UTF_8_stem, dutch_UTF_8_close_env),
    sm!("english", PG_UTF8, stem_utf8_english, english_UTF_8_create_env, english_UTF_8_stem, english_UTF_8_close_env),
    sm!("estonian", PG_UTF8, stem_utf8_estonian, estonian_UTF_8_create_env, estonian_UTF_8_stem, estonian_UTF_8_close_env),
    sm!("finnish", PG_UTF8, stem_utf8_finnish, finnish_UTF_8_create_env, finnish_UTF_8_stem, finnish_UTF_8_close_env),
    sm!("french", PG_UTF8, stem_utf8_french, french_UTF_8_create_env, french_UTF_8_stem, french_UTF_8_close_env),
    sm!("german", PG_UTF8, stem_utf8_german, german_UTF_8_create_env, german_UTF_8_stem, german_UTF_8_close_env),
    sm!("greek", PG_UTF8, stem_utf8_greek, greek_UTF_8_create_env, greek_UTF_8_stem, greek_UTF_8_close_env),
    sm!("hindi", PG_UTF8, stem_utf8_hindi, hindi_UTF_8_create_env, hindi_UTF_8_stem, hindi_UTF_8_close_env),
    sm!("hungarian", PG_UTF8, stem_utf8_hungarian, hungarian_UTF_8_create_env, hungarian_UTF_8_stem, hungarian_UTF_8_close_env),
    sm!("indonesian", PG_UTF8, stem_utf8_indonesian, indonesian_UTF_8_create_env, indonesian_UTF_8_stem, indonesian_UTF_8_close_env),
    sm!("irish", PG_UTF8, stem_utf8_irish, irish_UTF_8_create_env, irish_UTF_8_stem, irish_UTF_8_close_env),
    sm!("italian", PG_UTF8, stem_utf8_italian, italian_UTF_8_create_env, italian_UTF_8_stem, italian_UTF_8_close_env),
    sm!("lithuanian", PG_UTF8, stem_utf8_lithuanian, lithuanian_UTF_8_create_env, lithuanian_UTF_8_stem, lithuanian_UTF_8_close_env),
    sm!("nepali", PG_UTF8, stem_utf8_nepali, nepali_UTF_8_create_env, nepali_UTF_8_stem, nepali_UTF_8_close_env),
    sm!("norwegian", PG_UTF8, stem_utf8_norwegian, norwegian_UTF_8_create_env, norwegian_UTF_8_stem, norwegian_UTF_8_close_env),
    sm!("porter", PG_UTF8, stem_utf8_porter, porter_UTF_8_create_env, porter_UTF_8_stem, porter_UTF_8_close_env),
    sm!("portuguese", PG_UTF8, stem_utf8_portuguese, portuguese_UTF_8_create_env, portuguese_UTF_8_stem, portuguese_UTF_8_close_env),
    sm!("romanian", PG_UTF8, stem_utf8_romanian, romanian_UTF_8_create_env, romanian_UTF_8_stem, romanian_UTF_8_close_env),
    sm!("russian", PG_UTF8, stem_utf8_russian, russian_UTF_8_create_env, russian_UTF_8_stem, russian_UTF_8_close_env),
    sm!("serbian", PG_UTF8, stem_utf8_serbian, serbian_UTF_8_create_env, serbian_UTF_8_stem, serbian_UTF_8_close_env),
    sm!("spanish", PG_UTF8, stem_utf8_spanish, spanish_UTF_8_create_env, spanish_UTF_8_stem, spanish_UTF_8_close_env),
    sm!("swedish", PG_UTF8, stem_utf8_swedish, swedish_UTF_8_create_env, swedish_UTF_8_stem, swedish_UTF_8_close_env),
    sm!("tamil", PG_UTF8, stem_utf8_tamil, tamil_UTF_8_create_env, tamil_UTF_8_stem, tamil_UTF_8_close_env),
    sm!("turkish", PG_UTF8, stem_utf8_turkish, turkish_UTF_8_create_env, turkish_UTF_8_stem, turkish_UTF_8_close_env),
    sm!("yiddish", PG_UTF8, stem_utf8_yiddish, yiddish_UTF_8_create_env, yiddish_UTF_8_stem, yiddish_UTF_8_close_env),
    sm!("english", PG_SQL_ASCII, stem_iso_8859_1_english, english_ISO_8859_1_create_env, english_ISO_8859_1_stem, english_ISO_8859_1_close_env),
];

pub struct DictSnowball {
    z: *mut SN_env,
    stem: unsafe fn(*mut SN_env) -> c_int,
    close: unsafe fn(*mut SN_env),
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
    close: unsafe fn(*mut SN_env),
    needrecode: bool,
}

#[cfg(test)]
pub(crate) static STEMMERS_CLOSED: core::sync::atomic::AtomicUsize =
    core::sync::atomic::AtomicUsize::new(0);

// C pallocs the stemmer into the dictionary context, so an error after
// locate_stem_module reclaims it (dict_snowball.c:253-260).
impl Drop for Located {
    fn drop(&mut self) {
        if !self.z.is_null() {
            // SAFETY: z came from this module's create and is closed once.
            unsafe { (self.close)(self.z) };
            #[cfg(test)]
            STEMMERS_CLOSED.fetch_add(1, core::sync::atomic::Ordering::SeqCst);
        }
    }
}

fn locate_stem_module(lang: &[u8]) -> PgResult<Located> {
    let db_enc = ::mbutils::GetDatabaseEncoding();
    for m in &STEMMER_MODULES {
        if (m.enc == PG_SQL_ASCII || m.enc == db_enc) && eq_strcasecmp(m.name, lang) {
            // SAFETY: generated stemmer constructor allocating via crate::mem.
            let z = unsafe { (m.create)() };
            return Ok(Located { z, stem: m.stem, close: m.close, needrecode: false });
        }
    }
    for m in &STEMMER_MODULES {
        if m.enc == PG_UTF8 && eq_strcasecmp(m.name, lang) {
            // SAFETY: as above.
            let z = unsafe { (m.create)() };
            return Ok(Located { z, stem: m.stem, close: m.close, needrecode: true });
        }
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

    let Some(mut located) = located else {
        return Err(invalid_param("missing Language parameter").into());
    };

    Ok(DictSnowball {
        z: core::mem::replace(&mut located.z, core::ptr::null_mut()),
        stem: located.stem,
        close: located.close,
        stoplist: stoplist.unwrap_or(StopList { stop: PgVec::new_in(mcx) }),
        needrecode: located.needrecode,
    })
}

// SAFETY: `p` is a DictSnowball built by dsnowball_init, released once.
pub unsafe fn dsnowball_release(p: usize) {
    unsafe {
        let d = &mut *(p as *mut DictSnowball);
        (d.close)(d.z);
        d.z = core::ptr::null_mut();
        core::ptr::drop_in_place(d);
    }
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
