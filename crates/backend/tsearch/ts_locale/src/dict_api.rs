use ::mcx::{Mcx, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR};

use crate::public::{DictSubState, TsLexeme};

// fmgr 'internal' dict contract: init(arg0=*const DictInitData) -> *mut state;
// lexize(*mut state, *const u8 token, i32 len, *mut DictSubState|null)
//   -> *mut LexizeResult in fcinfo.result_mcx(); zero Datum = C NULL (not
//   recognized); empty LexizeResult = C's stopword array. dict_options mirror
//   deserialize_deflist (int_value Some iff C made a T_Integer node).
pub struct DictInitData<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub dict_options: PgVec<'mcx, (PgVec<'mcx, u8>, PgVec<'mcx, u8>)>,
    pub int_options: PgVec<'mcx, Option<i64>>,
}

pub struct LexizeResult<'mcx>(pub PgVec<'mcx, TsLexeme<'mcx>>);

pub type DictSubStatePtr = *mut DictSubState;

// SAFETY: callers pass a lexize-result Datum word whose result mcx is live.
pub unsafe fn lexize_result_ref<'a>(addr: usize) -> Option<&'a LexizeResult<'a>> {
    if addr == 0 {
        None
    } else {
        Some(unsafe { &*(addr as *const LexizeResult<'a>) })
    }
}

pub fn def_get_boolean(name: &[u8], value: &[u8], int_value: Option<i64>) -> PgResult<bool> {
    match int_value {
        Some(0) => return Ok(false),
        Some(1) => return Ok(true),
        Some(_) => {}
        None => {
            if value.eq_ignore_ascii_case(b"true") || value.eq_ignore_ascii_case(b"on") {
                return Ok(true);
            }
            if value.eq_ignore_ascii_case(b"false") || value.eq_ignore_ascii_case(b"off") {
                return Ok(false);
            }
        }
    }
    Err(PgError::error(format!(
        "{} requires a Boolean value",
        String::from_utf8_lossy(name)
    ))
    .with_sqlstate(ERRCODE_SYNTAX_ERROR)
    .into())
}
