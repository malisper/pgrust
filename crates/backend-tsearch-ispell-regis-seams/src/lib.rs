//! Seam declarations for the `backend-tsearch-ispell-regis` unit
//! (`tsearch/dict_ispell.c`): the `ispell` dictionary template's `init` and
//! `lexize` fmgr methods.
//!
//! The owning unit (`backend-tsearch-ispell-regis`) installs these from its
//! `init_seams()`; consumers reach them across the fmgr dispatch boundary
//! (the dictionary cache calls the template's registered C functions
//! `dispell_init` / `dispell_lexize`).

use backend_commands_define_seams::DefElemArg;
use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_tsearch::{DictISpell, TSLexeme};

seam_core::seam!(
    /// `dispell_init(dictoptions)` (dict_ispell.c): parse the
    /// `DictFile`/`AffFile`/`StopWords` options and build the ispell
    /// dictionary. Each option is `(defname, def->arg)`. The built
    /// [`DictISpell`] is allocated in `mcx` (C palloc's it in the dictionary's
    /// long-lived cache context). Bad/duplicate/missing options surface as
    /// `Err(ERRCODE_INVALID_PARAMETER_VALUE)`.
    pub fn dispell_init<'mcx>(
        mcx: Mcx<'mcx>,
        dictoptions: &[(String, Option<DefElemArg>)],
    ) -> PgResult<DictISpell<'mcx>>
);

seam_core::seam!(
    /// `dispell_lexize(d, in, len)` (dict_ispell.c): lowercase, normalize via
    /// the ispell dictionary, and drop stop words. `None` mirrors the C
    /// `PG_RETURN_POINTER(NULL)` (empty input or no normalization). The kept
    /// `TSLexeme`s are allocated in `mcx`.
    pub fn dispell_lexize<'mcx>(
        mcx: Mcx<'mcx>,
        d: &DictISpell<'_>,
        input: &[u8],
        len: i32,
    ) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>>
);
