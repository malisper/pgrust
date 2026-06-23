//! Port of the `ispell`-dictionary unit: `tsearch/dict_ispell.c` (the `ispell`
//! dictionary template) plus `tsearch/regis.c` (the fast regex subset the
//! ISpell affix machinery matches word endings with).
//!
//! `regis.c` is a self-contained leaf (only `ts_locale.c` / `mbutils.c`
//! helpers cross seams); `dict_ispell.c` reaches the unported `spell.c`
//! ISpell build pipeline, `ts_utils.c` stop-list/config-file helpers,
//! `formatting.c` case-folding, and `define.c` `DefElem` accessors through
//! their owner seam crates.

#![no_std]

extern crate alloc;

pub mod dict_ispell;
pub mod regis;

pub use dict_ispell::{dispell_init, dispell_lexize};
pub use regis::{rs_compile, rs_execute, rs_is_regis, Regis, RegisNode, RegisNodeKind};

/// Install every seam this unit owns (`backend-tsearch-ispell-regis-seams`):
/// the `ispell` dictionary template's `dispell_init` / `dispell_lexize` fmgr
/// methods. The regis entry points have no inward seams (their cyclic-free
/// callers depend on this crate directly).
pub fn init_seams() {
    ispell_regis_seams::dispell_init::set(dict_ispell::dispell_init);
    ispell_regis_seams::dispell_lexize::set(dict_ispell::dispell_lexize);
}
