//! `contrib/tablefunc` — normal_rand (SRF), the crosstab family, and the
//! connectby recursive walk. All run over the house SPI + funcapi SRF seams;
//! BuildTupleFromCStrings/AttInMetadata (funcapi.c) are ported locally in
//! `tupbuild` (tablefunc is their only consumer so far).

mod connectby;
mod crosstab;
mod normal_rand;
mod tupbuild;

use types_error::{PgError, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_fmgr::PGFunction;

const LIBRARY: &str = "tablefunc";

/// The UTF-8-only server-encoding carve (docs/design/carve-ratifications.md
/// §11): text that tablefunc would splice into the SQL it hands SPI (C
/// tablefunc.c:979-982/:1057-1061 `text_to_cstring` args, :1324 `SPI_getvalue` keys via
/// :1230/:1241 `quote_literal_cstr`, :360/:638-639 the crosstab source and categories
/// queries — all raw C strings) but that no `&str` can carry is refused
/// with the same typed 0A000 + HINT the tcop gate raises
/// (`non_utf8_query_error`), never a panic. Only reachable in a SQL_ASCII
/// database: textin validates UTF-8 everywhere else.
#[cold]
pub(crate) fn non_utf8_query_error() -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!(
                "query strings with non-ASCII characters are not supported yet in databases \
                 with encoding \"{}\"",
                mbutils::GetDatabaseEncodingName()
            ),
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_hint("Use a database with encoding \"UTF8\"."),
    )
}

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
