use ::mcx::{Mcx, PgVec};
use ::ts_locale::dict_api::{DictInitData, LexizeResult};
use ::ts_locale::{
    get_tsearch_config_filename, lowerstr, readstoplist, searchstoplist, StopList, TsLexeme,
};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use crate::IspellDict;

pub struct DictISpell {
    pub obj: IspellDict<'static>,
    pub stoplist: StopList<'static>,
}

fn invalid_param(message: String) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

pub fn dispell_init(init: &DictInitData<'static>) -> PgResult<DictISpell> {
    let mcx = init.mcx;
    let mut affloaded = false;
    let mut dictloaded = false;
    let mut stoploaded = false;

    let mut stoplist = StopList { stop: PgVec::new_in(mcx) };
    let mut obj = IspellDict::new(mcx);
    obj.ni_start_build()?;

    for (name, value) in &init.dict_options {
        if name.as_slice() == b"dictfile" {
            if dictloaded {
                return Err(invalid_param("multiple DictFile parameters".into()).into());
            }
            let path = get_tsearch_config_filename(mcx, value.as_slice(), "dict")?;
            obj.ni_import_dictionary(&path)?;
            dictloaded = true;
        } else if name.as_slice() == b"afffile" {
            if affloaded {
                return Err(invalid_param("multiple AffFile parameters".into()).into());
            }
            let path = get_tsearch_config_filename(mcx, value.as_slice(), "affix")?;
            obj.ni_import_affixes(&path)?;
            affloaded = true;
        } else if name.as_slice() == b"stopwords" {
            if stoploaded {
                return Err(invalid_param("multiple StopWords parameters".into()).into());
            }
            stoplist = readstoplist(mcx, Some(value.as_slice()), true)?;
            stoploaded = true;
        } else {
            return Err(invalid_param(format!(
                "unrecognized Ispell parameter: \"{}\"",
                String::from_utf8_lossy(name)
            ))
            .into());
        }
    }

    if affloaded && dictloaded {
        obj.ni_sort_dictionary()?;
        obj.ni_sort_affixes()?;
    } else if !affloaded {
        return Err(invalid_param("missing AffFile parameter".into()).into());
    } else {
        return Err(invalid_param("missing DictFile parameter".into()).into());
    }

    obj.ni_finish_build()?;

    Ok(DictISpell { obj, stoplist })
}

pub fn dispell_lexize<'mcx>(
    mcx: Mcx<'mcx>,
    d: &DictISpell,
    token: &[u8],
) -> PgResult<Option<LexizeResult<'mcx>>> {
    if token.is_empty() {
        return Ok(None);
    }

    let txt = lowerstr(mcx, token)?;
    let res = d.obj.ni_normalize_word(mcx, &txt)?;
    if res.is_empty() {
        return Ok(None);
    }

    let mut kept: PgVec<'mcx, TsLexeme<'mcx>> = PgVec::new_in(mcx);
    for lex in res {
        if searchstoplist(&d.stoplist, &lex.lexeme) {
            continue;
        }
        kept.try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<TsLexeme>()))?;
        kept.push(lex);
    }

    Ok(Some(LexizeResult(kept)))
}
