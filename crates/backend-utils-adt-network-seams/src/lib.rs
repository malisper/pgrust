//! Outward seam declarations for `utils/adt/network.c`.
//!
//! `network.c` owns `inet_client_addr` / `inet_server_addr` (and the `_port`
//! variants), `network_sortsupport`, and `network_subset_support`, but the
//! *external* substrate each of those reaches into lives in a not-yet-ported
//! neighbour subsystem. These slots model exactly those cross-subsystem reaches;
//! `backend-utils-adt-network` CALLS them (it never installs them), so they stay
//! uninstalled — a loud panic — until the real owner subsystem lands. That is
//! mirror-PG-and-panic, not a regression.
//!
//!   * [`session::resolve`] — `MyProcPort->{raddr,laddr}` +
//!     `pg_getnameinfo_all(NI_NUMERICHOST|NI_NUMERICSERV)` (libpq-be).
//!   * [`sortsupport::register`] — installing the comparator / abbrev callbacks
//!     into the live `SortSupportData` node and the HyperLogLog estimator
//!     (tuplesort / `lib/hyperloglog`).
//!   * [`planner::network_subset_support`] — building index-condition `OpExpr`
//!     trees via catalog lookups + `make_opclause` (planner / nodes / catalog).

#![allow(non_snake_case)]

use types_network::{ResolvedName, SessionEndpoint};

pub mod inet {
    use types_datum::datum::Datum;
    use types_error::PgResult;
    use types_network::inet_struct;

    seam_core::seam!(
        /// `DatumGetInetPP(X)` (utils/inet.h): detoast the `inet`/`cidr` varlena
        /// the bare-word [`Datum`] points at and return its
        /// [`inet_struct`] payload (`family` / `bits` / `ipaddr`). The selectivity
        /// estimators apply this to the `pg_statistic` MCV / histogram value
        /// arrays and the query `Const`, whose words point at inet varlenas in
        /// detoasted (syscache / `mcx`) memory. The fmgr/varlena envelope —
        /// `PG_DETOAST_DATUM` plus the `inet_struct` byte decode
        /// ([`inet_struct::from_datum_bytes`]) — is the deferred edge
        /// `backend-utils-adt-network` itself does not own (its functions take
        /// already-decoded `inet_struct` values), so this slot stays uninstalled
        /// (a loud panic) until the varlena-detoast owner lands. That is
        /// mirror-PG-and-panic, not a regression. `Err` carries any detoast
        /// `ereport(ERROR)`.
        pub fn datum_get_inet_pp(value: Datum) -> PgResult<inet_struct>
    );
}

pub mod session {
    use super::*;

    seam_core::seam!(
        /// Resolve one session endpoint's numeric host/port string from
        /// `MyProcPort` (`pg_getnameinfo_all`). `None` mirrors the C path that
        /// returns SQL NULL (Unix-domain socket / no connection): both
        /// `inet_client_addr` and `inet_client_port` test `ret != 0` after
        /// `pg_getnameinfo_all` and return NULL on failure.
        pub fn resolve(endpoint: SessionEndpoint) -> Option<ResolvedName>
    );
}

pub mod sortsupport {
    seam_core::seam!(
        /// Install the SortSupport comparator + abbreviation callbacks into the
        /// live `SortSupportData` node (and initialize the HyperLogLog estimator
        /// when `ssup.abbreviate`). Returns whether a registrar was wired; the
        /// default (uninstalled) is a faithful no-op, exactly as if sortsupport
        /// were never registered (the btree AM falls back to `network_cmp`).
        pub fn register() -> bool
    );
}

pub mod planner {
    seam_core::seam!(
        /// Inspect the `SupportRequestIndexCondition` request and, when it
        /// matches, build the derived index-condition `OpExpr` tree. Returns
        /// whether index conditions were derived; declining is always a valid
        /// planner answer, so the uninstalled default is a faithful "no".
        pub fn network_subset_support() -> bool
    );
}
