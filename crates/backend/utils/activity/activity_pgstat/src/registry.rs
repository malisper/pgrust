//! The process-global builtin kind table singleton and its registration entry
//! point.
//!
//! `pgstat.c` declares `pgstat_kind_builtin_infos[]` as a `static const`
//! designated-initializer array assembled at compile time. The idiomatic model
//! assembles it once at startup into a `OnceLock`, populated by each per-kind
//! owner crate calling [`register_builtin_kind`] from its `init_seams()`. The
//! lookup [`pgstat_get_kind_info`] mirrors `pgstat.c`'s function of the same
//! name (builtin half; the custom-kind registry is a follow-on).

use std::sync::{Mutex, OnceLock};

use ::types_pgstat::activity_pgstat::PgStat_Kind;

use crate::kind_info::{KindInfoBuilder, PgStat_KindInfoFull, PgStat_KindInfoTable};

/// The being-assembled table. Per-kind crates register into this before the
/// table is sealed. A `Mutex` guards concurrent registration during startup;
/// after [`seal_kind_table`] it is read-only through [`kind_table`].
static BUILDING: Mutex<Option<PgStat_KindInfoTable>> = Mutex::new(None);

/// The sealed, read-only builtin kind table.
static SEALED: OnceLock<PgStat_KindInfoTable> = OnceLock::new();

fn building<R>(f: impl FnOnce(&mut PgStat_KindInfoTable) -> R) -> R {
    let mut guard = BUILDING.lock().unwrap();
    let table = guard.get_or_insert_with(PgStat_KindInfoTable::new);
    f(table)
}

/// Register one builtin kind's full descriptor (metadata + callbacks).
///
/// This is the single API each per-kind owner crate calls from its
/// `init_seams()` to populate its slot in `pgstat_kind_builtin_infos[]`. Panics
/// if called after the table is sealed, or if the kind is registered twice.
pub fn register_builtin_kind(kind: PgStat_Kind, full: PgStat_KindInfoFull) {
    assert!(
        SEALED.get().is_none(),
        "pgstat kind table already sealed; register kinds before sealing"
    );
    building(|t| t.register(kind, full));
}

/// Convenience: register from a [`KindInfoBuilder`].
pub fn register(builder: KindInfoBuilder) {
    let (kind, full) = builder.build();
    register_builtin_kind(kind, full);
}

/// Seal the builtin kind table, moving it from the mutable building stage to
/// the read-only `OnceLock`. Called once `pgstat.c` is ported, after every
/// per-kind crate has registered. Idempotent-safe: panics if sealed twice.
pub fn seal_kind_table() {
    let mut guard = BUILDING.lock().unwrap();
    let table = guard.take().unwrap_or_default();
    if SEALED.set(table).is_err() {
        panic!("pgstat kind table sealed twice");
    }
}

/// The sealed builtin kind table. Panics if accessed before [`seal_kind_table`]
/// (the C array is `const` and always present; the runtime-assembled model must
/// be sealed first).
pub fn kind_table() -> &'static PgStat_KindInfoTable {
    SEALED
        .get()
        .expect("pgstat kind table accessed before seal_kind_table()")
}

/// `pgstat_get_kind_info(kind)` (`pgstat.c`), builtin half — the per-kind
/// descriptor for a builtin `kind`, or `None`. (The custom-kind registry is a
/// follow-on; this resolves the builtin table only.)
pub fn pgstat_get_kind_info(kind: PgStat_Kind) -> Option<&'static PgStat_KindInfoFull> {
    kind_table().get(kind)
}

/// Test-only: drain the building stage so a test can re-assemble the table in
/// isolation. `SEALED` is a `OnceLock` and cannot be reset, so tests read back
/// the building stage (via [`take_building_kind_for_test`]) without sealing,
/// leaving the production `OnceLock` free. Not part of the production API.
#[cfg(test)]
pub(crate) fn reset_for_test() {
    *BUILDING.lock().unwrap() = None;
}

/// Test-only: remove and return one builtin kind's descriptor from the building
/// stage, so a test can inspect what [`register`] installed without sealing.
#[cfg(test)]
pub(crate) fn take_building_kind_for_test(
    kind: PgStat_Kind,
) -> Option<crate::kind_info::PgStat_KindInfoFull> {
    let mut guard = BUILDING.lock().unwrap();
    guard.as_mut().and_then(|t| t.take_for_test(kind))
}
