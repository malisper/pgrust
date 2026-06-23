//! Port of `src/backend/tsearch/dict_simple.c` — the `simple` dictionary
//! template: lowercase and check for a stopword.

use alloc::format;
use alloc::string::String;

use define_seams::{def_get_boolean, def_get_string, DefElemArg};
use ts_utils_seams::{readstoplist, searchstoplist};
use ::formatting_seams::str_tolower;

use mcx::{Mcx, PgString, PgVec};
use ::types_error::PgResult;
use tsearch::{DictSimple, StopList, TSLexeme};

use crate::{invalid_param, DEFAULT_COLLATION_OID};

/// `dsimple_init(PG_FUNCTION_ARGS)`: parse `stopwords`/`accept`, build the
/// `simple` dictionary. `dictoptions` is the C `List *` of `DefElem`s, each
/// `(defname, def->arg)`. The built [`DictSimple`] is allocated in `mcx`.
pub fn dsimple_init<'mcx>(
    mcx: Mcx<'mcx>,
    dictoptions: &[(String, Option<DefElemArg>)],
) -> PgResult<DictSimple<'mcx>> {
    // d = palloc0(sizeof(DictSimple)); the stop list starts empty.
    let mut stoplist = StopList {
        stop: PgVec::new_in(mcx),
    };
    let mut stoploaded = false;
    let mut acceptloaded = false;
    let mut accept = true; // default

    for (defname, arg) in dictoptions {
        if defname == "stopwords" {
            if stoploaded {
                return Err(invalid_param("multiple StopWords parameters"));
            }
            // readstoplist(defGetString(defel), &d->stoplist, str_tolower);
            let base = def_get_string::call(mcx, defname.clone(), arg.clone())?;
            stoplist = readstoplist::call(mcx, base.as_bytes(), true)?;
            stoploaded = true;
        } else if defname == "accept" {
            if acceptloaded {
                return Err(invalid_param("multiple Accept parameters"));
            }
            accept = def_get_boolean::call(defname.clone(), arg.clone())?;
            acceptloaded = true;
        } else {
            return Err(invalid_param(format!(
                "unrecognized simple dictionary parameter: \"{defname}\""
            )));
        }
    }

    Ok(DictSimple { stoplist, accept })
}

/// `dsimple_lexize(PG_FUNCTION_ARGS)`: lowercase, drop stop words, accept/reject.
///
/// `input`/`len` are the C `char *in` / `int32 len`. Returns `None` for the C
/// `PG_RETURN_POINTER(NULL)` (unrecognized word with `accept=false`),
/// `Some(vec![])` for the empty `palloc0(2*TSLexeme)` (reject as stopword), and
/// `Some(vec)` for the accepted single lexeme. Lexemes allocated in `mcx`.
pub fn dsimple_lexize<'mcx>(
    mcx: Mcx<'mcx>,
    d: &DictSimple<'_>,
    input: &[u8],
    len: i32,
) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>> {
    // txt = str_tolower(in, len, DEFAULT_COLLATION_OID);
    let in_bytes = slice_from_in(input, len);
    let txt = str_tolower::call(mcx, in_bytes, DEFAULT_COLLATION_OID)?;

    // if (*txt == '\0' || searchstoplist(&(d->stoplist), txt))
    if txt.is_empty() || searchstoplist::call(&d.stoplist, &txt) {
        // reject as stopword: res = palloc0(sizeof(TSLexeme) * 2);
        // (an empty, non-null result array)
        return Ok(Some(PgVec::new_in(mcx)));
    } else if d.accept {
        // accept: res[0].lexeme = txt;
        let mut res: PgVec<'mcx, TSLexeme<'mcx>> = PgVec::new_in(mcx);
        let lexeme = PgString::from_str_in(
            core::str::from_utf8(&txt).map_err(|_| crate::elog_error("dsimple_lexize: non-UTF-8 lowercased lexeme"))?,
            mcx,
        )?;
        res.try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<TSLexeme>()))?;
        res.push(TSLexeme {
            nvariant: 0,
            flags: 0,
            lexeme,
        });
        return Ok(Some(res));
    }

    // report as unrecognized: PG_RETURN_POINTER(NULL);
    Ok(None)
}

/// The C lexize methods receive `char *in` and `int32 len`; this slices `len`
/// bytes off the (possibly longer / NUL-terminated) `input` view. C ignores
/// bytes past `len`.
pub(crate) fn slice_from_in(input: &[u8], len: i32) -> &[u8] {
    if len <= 0 {
        &[]
    } else {
        let n = (len as usize).min(input.len());
        &input[..n]
    }
}
