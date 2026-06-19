//! Seam declarations for the PL/pgSQL compiler unit (`pl_comp.c`)'s
//! scanner-callback surface.
//!
//! `pl_scanner.c`'s `plpgsql_yylex` postparses each identifier (or compound
//! dotted identifier) by calling back into the compiler to resolve it against
//! the function's variable namespace: `plpgsql_parse_word` for a simple name,
//! `plpgsql_parse_dblword` for `A.B`, and `plpgsql_parse_tripword` for `A.B.C`.
//! Those resolvers live in `pl_comp.c` (the `backend-pl-plpgsql-comp` unit),
//! which in turn consumes the scanner's keyword tables — a cycle. The scanner
//! therefore reaches them through these seams; the compiler unit installs them
//! from its `init_seams()` when it lands. Until then a call panics loudly
//! (mirror-PG-and-panic).
//!
//! ## Modeling the C out-parameter contract
//!
//! Each C resolver returns `bool` (matched a datum?) and fills *one* of two
//! out-parameters:
//!   * on `true`  -> `*wdatum` (a [`PLwdatum`]) is filled, identifying the
//!     resolved variable;
//!   * on `false` -> the word/cword out-parameter (`*word` [`PLword`] for the
//!     simple case, `*cword` [`PLcword`] for the compound cases) is filled with
//!     the un-resolved identifier text the grammar will report.
//!
//! We carry that as a [`WordResolution`] tagged result so the (mutually
//! exclusive) out-parameters are expressed by construction rather than by a
//! `bool` plus two maybe-initialized slots.

use types_error::PgResult;
use types_plpgsql::{PLcword, PLwdatum, PLword};

/// Result of resolving a single identifier (`plpgsql_parse_word`).
///
/// `Datum` mirrors the C `true` return (the `*wdatum` out-parameter was
/// filled); `Word` mirrors the `false` return (the `*word` out-parameter was
/// filled with the literal identifier).
pub enum WordResolution {
    Datum(PLwdatum),
    Word(PLword),
}

/// Result of resolving a compound (dotted) identifier (`plpgsql_parse_dblword`
/// / `plpgsql_parse_tripword`).
///
/// `Datum` mirrors the C `true` return (`*wdatum` filled); `Cword` mirrors the
/// `false` return (`*cword` filled with the dotted-name component list).
pub enum CwordResolution {
    Datum(PLwdatum),
    Cword(PLcword),
}

seam_core::seam!(
    /// `plpgsql_parse_word(word1, yytxt, lookup, wdatum, word)` (`pl_comp.c`):
    /// postparse a single identifier. `word1` is the (possibly downcased,
    /// truncated) identifier; `yytxt` is the original token text used to test
    /// whether the identifier was double-quoted; `lookup` requests a variable
    /// lookup (suppressed at statement start where the name can't be a
    /// variable). Returns [`WordResolution::Datum`] when the name resolves to a
    /// PL/pgSQL variable, else [`WordResolution::Word`].
    pub fn plpgsql_parse_word(
        word1: &str,
        yytxt: &str,
        lookup: bool,
    ) -> PgResult<WordResolution>
);

seam_core::seam!(
    /// `plpgsql_parse_dblword(word1, word2, wdatum, cword)` (`pl_comp.c`):
    /// the same lookup for a two-component dotted name `word1.word2`.
    pub fn plpgsql_parse_dblword(word1: &str, word2: &str) -> PgResult<CwordResolution>
);

seam_core::seam!(
    /// `plpgsql_parse_tripword(word1, word2, word3, wdatum, cword)`
    /// (`pl_comp.c`): the same lookup for a three-component dotted name
    /// `word1.word2.word3`.
    pub fn plpgsql_parse_tripword(
        word1: &str,
        word2: &str,
        word3: &str,
    ) -> PgResult<CwordResolution>
);
