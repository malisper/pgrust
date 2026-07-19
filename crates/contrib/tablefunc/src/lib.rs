//! `contrib/tablefunc` — normal_rand (SRF), the crosstab family, and the
//! connectby recursive walk. All run over the house SPI + funcapi SRF seams;
//! BuildTupleFromCStrings/AttInMetadata (funcapi.c) are ported locally in
//! `tupbuild` (tablefunc is their only consumer so far).

mod connectby;
mod crosstab;
mod normal_rand;
mod tupbuild;

use types_fmgr::PGFunction;

const LIBRARY: &str = "tablefunc";

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "normal_rand" => normal_rand::fc_normal_rand,
        "crosstab" => crosstab::fc_crosstab,
        "crosstab_hash" => crosstab::fc_crosstab_hash,
        "connectby_text" => connectby::fc_connectby_text,
        "connectby_text_serial" => connectby::fc_connectby_text_serial,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}
