// utility.c — utility-statement classifiers + ProcessUtility dispatch (PG 18.3).
#![allow(non_snake_case)]

use types_error::ErrorLocation;

pub mod classify;
pub mod commandtag;
pub mod consts;
pub mod dispatch;
pub mod loglevel;
pub mod returns;
#[cfg(test)]
mod tests;

pub use classify::{
    CheckRestrictedOperation, ClassifyUtilityCommandAsReadOnly, CommandIsReadOnly,
    PreventCommandDuringRecovery,
};
pub use commandtag::CreateCommandTag;
pub use dispatch::{standard_ProcessUtility, tap_process_utility_enter, tap_process_utility_leave, ProcessUtility};
pub use loglevel::GetCommandLogLevel;
pub use returns::{UtilityContainsQuery, UtilityReturnsTuples, UtilityTupleDescriptor};
pub use xact::{PreventCommandIfParallelMode, PreventCommandIfReadOnly};

pub fn init_seams() {
    utility_seams::create_command_tag::set(CreateCommandTag);
    utility_seams::get_command_log_level::set(GetCommandLogLevel);
    utility_seams::utility_returns_tuples::set(UtilityReturnsTuples);
    utility_seams::utility_tuple_descriptor::set(UtilityTupleDescriptor);
    utility_seams::process_utility::set(ProcessUtility);
    utility_seams::utility_contains_query::set(UtilityContainsQuery);
}

#[track_caller]
pub(crate) fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

#[cold]
#[inline(never)]
pub(crate) fn payload_gap(func: &str, node: &str) -> ! {
    panic!("{func} (utility.c): {node} payload not in types_nodes (grammar lane)")
}

#[cold]
#[inline(never)]
pub(crate) fn handler_gap(what: &str) -> ! {
    panic!("standard_ProcessUtility (utility.c): {what} not ported")
}

// Clean 0A000 for unported-feature utility lanes: user-reachable statement
// shapes whose handler isn't ported must raise, not panic (utility dispatch
// is unwind-safe here; the connection survives and later statements work).
#[cold]
#[inline(never)]
pub(crate) fn handler_unsupported(what: &str) -> Box<types_error::PgError> {
    Box::new(
        ::elog::ereport(types_error::ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("{what} is not supported yet"))
            .into_error()
            .with_error_location(loc("standard_ProcessUtility")),
    )
}
