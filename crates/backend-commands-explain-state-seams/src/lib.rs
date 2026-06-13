//! Seam declarations for `commands/explain_state.c`'s extension callbacks.
//!
//! `explain_state.c` owns the EXPLAIN extension registry (the file-scope
//! `ExplainExtensionOptionArray` and the `explain_validate_options_hook`
//! global). The values it stores are foreign function pointers registered by a
//! loaded extension (e.g. `pg_overexplain`): the per-option
//! `ExplainOptionHandler` and the `explain_validate_options_hook`. Invoking one
//! crosses into that extension's code, so the dispatch goes through these
//! `call_*` seams.
//!
//! Each handle is a [`Copy`] opaque token the extension hands to
//! `RegisterExtensionExplainOption` / `set_explain_validate_options_hook`; the
//! ported `explain_state` crate stores it in the registry and invokes it here
//! when the matching option is parsed. No extension is ported yet, so nothing
//! installs these and a call panics loudly — which is correct: the in-core
//! option dispatch never reaches them (no handler can be registered), they fire
//! only once an extension lands and installs them.
//!
//! Both callbacks can `ereport(ERROR)` (extension validation), so the seams
//! return `PgResult<()>` mirroring the C `void` failure surface.

use seam_core::seam;
use types_cluster::ParseState;
use types_error::PgResult;
use types_explain::ExplainState;
use types_parsenodes::DefElem;

/// `ExplainOptionHandler` (commands/explain_state.h) —
/// `void (*)(ExplainState *, DefElem *, ParseState *)`. An opaque handle to the
/// extension's per-option handler; the registry stores it by option name.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct ExplainOptionHandler(pub u64);

/// `explain_validate_options_hook_type` (commands/explain_state.h) —
/// `void (*)(ExplainState *, List *, ParseState *)`. An opaque handle to the
/// plugin's post-parse cross-option validator.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct ExplainValidateOptionsHook(pub u64);

seam!(
    /// `(*handler)(es, opt, pstate)` — invoke an extension's registered EXPLAIN
    /// option handler (`ApplyExtensionExplainOption`, explain_state.c:374).
    pub fn call_option_handler<'mcx>(
        handler: ExplainOptionHandler,
        es: &mut ExplainState<'mcx>,
        opt: &DefElem,
        pstate: &mut ParseState,
    ) -> PgResult<()>
);

seam!(
    /// `(*explain_validate_options_hook)(es, options, pstate)` — invoke the
    /// plugin's post-parse option validator (`ParseExplainOptionList`,
    /// explain_state.c:206).
    pub fn call_validate_options_hook<'mcx>(
        hook: ExplainValidateOptionsHook,
        es: &mut ExplainState<'mcx>,
        options: &[DefElem],
        pstate: &mut ParseState,
    ) -> PgResult<()>
);
