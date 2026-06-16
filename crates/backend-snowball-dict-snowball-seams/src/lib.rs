//! Seam declarations for the `backend-snowball-dict-snowball` unit
//! (`snowball/dict_snowball.c`): the `snowball` dictionary template's `init`
//! and `lexize` fmgr methods.
//!
//! The owning unit (`backend-snowball-dict-snowball`) installs these from its
//! `init_seams()`; consumers reach them across the fmgr dispatch boundary
//! (the dictionary cache calls the template's registered C functions
//! `dsnowball_init` / `dsnowball_lexize`).

use backend_commands_define_seams::DefElemArg;
use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_tsearch::{DictSnowball, TSLexeme};

seam_core::seam!(
    /// `dsnowball_init(dictoptions)` (dict_snowball.c): parse the
    /// `Language`/`StopWords` options, locate the per-language Snowball stemmer
    /// module for the database encoding, and load the optional stop list. Each
    /// option is `(defname, def->arg)`. The built [`DictSnowball`] is allocated
    /// in `mcx` (C palloc's it in the dictionary's long-lived cache context;
    /// `dictCtx = CurrentMemoryContext`). Bad/duplicate/missing options and a
    /// missing stemmer surface as `Err`.
    pub fn dsnowball_init<'mcx>(
        mcx: Mcx<'mcx>,
        dictoptions: &[(String, Option<DefElemArg>)],
    ) -> PgResult<DictSnowball<'mcx>>
);

seam_core::seam!(
    /// `dsnowball_lexize(d, in, len)` (dict_snowball.c): lowercase the token,
    /// drop stop words, then run the Snowball stemmer (recoding to/from UTF-8
    /// when `d.needrecode`). Returns the single stemmed lexeme (the C
    /// `palloc0(sizeof(TSLexeme)*2)` array with one populated entry); an empty
    /// or stop-word token yields an empty result (the C lexeme stays NULL). The
    /// kept `TSLexeme` is allocated in `mcx`.
    pub fn dsnowball_lexize<'mcx>(
        mcx: Mcx<'mcx>,
        d: &DictSnowball<'_>,
        input: &[u8],
        len: i32,
    ) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>>
);
