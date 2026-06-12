//! Function-manager call-interface vocabulary (`fmgr.h`), trimmed.

use crate::funcapi::ReturnSetInfo;

/// `FunctionCallInfoBaseData` (fmgr.h) — the call frame every fmgr-called
/// function receives (`FunctionCallInfo` is `FunctionCallInfoBaseData *`).
/// Trimmed (docs/types.md rule 3) to the fields current ports consume; the
/// fmgr port widens it (`flinfo`, `context`, `fncollation`, `isnull`,
/// `nargs`, `args`).
#[derive(Debug, Default)]
pub struct FunctionCallInfoBaseData<'mcx> {
    /// `fmNodePtr resultinfo` — extra info about the result. For a
    /// set-returning call C points this at a `ReturnSetInfo` node; the owned
    /// model stores that node inline (`None` is the C `NULL`).
    pub resultinfo: Option<ReturnSetInfo<'mcx>>,
}
