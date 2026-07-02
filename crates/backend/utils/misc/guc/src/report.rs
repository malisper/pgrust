use std::cell::Cell;

use types_dest::CommandDest;
use types_guc::{GUC_REPORT, PGC_INTERNAL, PGC_S_OVERRIDE};

use crate::registry::show_guc_option;
use crate::store::{with_store, with_store_mut};

// PqMsg_ParameterStatus (libpq/protocol.h).
const PQMSG_PARAMETER_STATUS: u8 = b'S';

thread_local! {
    static REPORTING_ENABLED: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn set_reporting_enabled(v: bool) {
    REPORTING_ENABLED.set(v);
}

// BeginReportingGUCOptions (guc.c:2546).
pub fn begin_reporting_guc_options() {
    if elog::config::where_to_send_output() != CommandDest::Remote {
        return;
    }

    REPORTING_ENABLED.set(true);

    // in_hot_standby hack: flip the GUC true if recovery is in progress. The
    // recovery seam is installed with the xlog unit; until then the boot-false
    // value stands (single-node primary behavior).
    if transam_xlog_seams::recovery_in_progress::is_installed()
        && transam_xlog_seams::recovery_in_progress::call()
    {
        let _ = crate::SetConfigOption("in_hot_standby", Some("true"), PGC_INTERNAL, PGC_S_OVERRIDE);
    }

    let pending: Vec<(String, String)> = with_store(|reg| {
        reg.iter()
            .filter(|var| var.gen().flags & GUC_REPORT != 0)
            .filter_map(|var| {
                let val = show_guc_option(var, false);
                needs_report(var.gen().last_reported.as_deref(), &val)
                    .then(|| (var.name().to_string(), val))
            })
            .collect()
    })
    .unwrap_or_default();

    transmit_and_remember(&pending);
}

// ReportChangedGUCOptions (guc.c:2596): drains guc_report_list — O(changed),
// empty (no scan) for a statement that changed no reportable GUC.
pub fn report_changed_guc_options() {
    if !REPORTING_ENABLED.get() {
        return;
    }

    // in_hot_standby can only transition true -> false. Read the backing bool
    // directly (the accessor-slot fn pointer defeats inlining; the install is
    // always guc_tables::backing, so this is the same load C does).
    if guc_tables::backing::in_hot_standby_guc()
        && transam_xlog_seams::recovery_in_progress::is_installed()
        && !transam_xlog_seams::recovery_in_progress::call()
    {
        let _ =
            crate::SetConfigOption("in_hot_standby", Some("false"), PGC_INTERNAL, PGC_S_OVERRIDE);
    }

    // Nothing noted since the last drain: one Cell load, C's empty
    // slist_is_empty(&guc_report_list) shape — no store borrow, no mem::take.
    if !crate::store::report_pending_hint() {
        return;
    }
    report_changed_haswork();
}

#[cold]
#[inline(never)]
fn report_changed_haswork() {
    crate::store::set_report_pending_hint(false);
    let drained: Vec<usize> = with_store_mut(|reg| reg.drain_report_list()).unwrap_or_default();
    if drained.is_empty() {
        return;
    }

    let pending: Vec<(String, String)> = with_store(|reg| {
        drained
            .iter()
            .filter_map(|&idx| {
                let var = &reg[idx];
                let val = show_guc_option(var, false);
                needs_report(var.gen().last_reported.as_deref(), &val)
                    .then(|| (var.name().to_string(), val))
            })
            .collect()
    })
    .unwrap_or_default();

    transmit_and_remember(&pending);
}

// ReportGUCOption's transmit + last_reported refresh (guc.c:2634), applied
// after the store borrow drops (the byte sink may re-enter the store).
fn transmit_and_remember(pending: &[(String, String)]) {
    for (name, val) in pending {
        let mut body = Vec::with_capacity(name.len() + val.len() + 2);
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(val.as_bytes());
        body.push(0);
        let _ = pqcomm_seams::pq_putmessage::call(PQMSG_PARAMETER_STATUS, &body);
    }
    if pending.is_empty() {
        return;
    }
    with_store_mut(|reg| {
        for (name, val) in pending {
            if let Some(var) = reg.find_option_mut(name) {
                var.gen_mut().last_reported = Some(val.clone());
            }
        }
    });
}

fn needs_report(last_reported: Option<&str>, val: &str) -> bool {
    last_reported != Some(val)
}
