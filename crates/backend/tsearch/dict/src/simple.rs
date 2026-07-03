use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::ts_locale::dict_api::{def_get_boolean, DictInitData, LexizeResult};
use ::ts_locale::{lowerstr, readstoplist, searchstoplist, StopList, TsLexeme};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

pub struct DictSimple {
    stoplist: StopList<'static>,
    accept: bool,
}

#[cold]
pub(crate) fn invalid_param(msg: String) -> Box<PgError> {
    PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE).into()
}

pub fn dsimple_init(init: &DictInitData<'static>) -> PgResult<DictSimple> {
    let mut stoplist = None;
    let mut accept = None;
    for (i, (name, value)) in init.dict_options.iter().enumerate() {
        if name.as_slice() == b"stopwords" {
            if stoplist.is_some() {
                return Err(invalid_param("multiple StopWords parameters".into()));
            }
            stoplist = Some(readstoplist(init.mcx, Some(value.as_slice()), true)?);
        } else if name.as_slice() == b"accept" {
            if accept.is_some() {
                return Err(invalid_param("multiple Accept parameters".into()));
            }
            accept = Some(def_get_boolean(name, value, init.int_options[i])?);
        } else {
            return Err(invalid_param(format!(
                "unrecognized simple dictionary parameter: \"{}\"",
                String::from_utf8_lossy(name)
            )));
        }
    }
    Ok(DictSimple {
        stoplist: stoplist.unwrap_or(StopList { stop: PgVec::new_in(init.mcx) }),
        accept: accept.unwrap_or(true),
    })
}

pub fn dsimple_lexize<'mcx>(
    mcx: Mcx<'mcx>,
    d: &DictSimple,
    token: &[u8],
) -> PgResult<Option<LexizeResult<'mcx>>> {
    let txt = lowerstr(mcx, token)?;
    if txt.is_empty() || searchstoplist(&d.stoplist, &txt) {
        return Ok(Some(LexizeResult(PgVec::new_in(mcx))));
    }
    if d.accept {
        let mut out = PgVec::new_in(mcx);
        out.push(TsLexeme { nvariant: 0, flags: 0, lexeme: txt });
        return Ok(Some(LexizeResult(out)));
    }
    Ok(None)
}
