use guc::{NewGUCNestLevel, AtEOXact_GUC, set_config_option, GUC_ACTION_SAVE};
use types_error::ErrorLevel;
use types_guc::{PGC_S_SESSION, PGC_USERSET};

// set_transmission_modes (postgres_fdw.c): pin datestyle/intervalstyle/
// extra_float_digits/search_path so remote constants print portably.
// Divergence: C guards datestyle/intervalstyle/extra_float_digits behind a
// "already at the target?" check to avoid a redundant GUC stack entry; we set
// unconditionally (no cheap getters for the three session vars), which yields
// identical effective values during deparse — only an extra save/restore.
pub fn set_transmission_modes() -> i32 {
    let nestlevel = NewGUCNestLevel();
    let set = |name: &str, val: &str| {
        let _ = set_config_option(
            name,
            Some(val),
            PGC_USERSET,
            PGC_S_SESSION,
            GUC_ACTION_SAVE,
            true,
            ErrorLevel(0),
            false,
        );
    };
    set("datestyle", "ISO");
    set("intervalstyle", "postgres");
    set("extra_float_digits", "3");
    set("search_path", "pg_catalog");
    nestlevel
}

pub fn reset_transmission_modes(nestlevel: i32) {
    AtEOXact_GUC(true, nestlevel);
}
