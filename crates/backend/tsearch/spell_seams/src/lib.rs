//! Seam declarations for the `backend-tsearch-spell` unit
//! (`tsearch/dicts/spell.c`): the ISpell dictionary build pipeline and word
//! normalizer.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! C embeds an `IspellDict obj` inside the caller's `DictISpell` and threads
//! `&d->obj` through every step. Here the build state lives behind the opaque
//! [`SpellHandle`](::tsearch::SpellHandle) the owner mints in
//! `NIStartBuild` and resolves to its real `IspellDict`. Every step can
//! `ereport(ERROR)` (file-read failures, parse errors, OOM), so all return
//! `PgResult`. `NINormalizeWord` allocates its `TSLexeme[]` result in the
//! caller's current context, so it takes the target `Mcx<'mcx>` and its output
//! carries `'mcx`; `None` mirrors the C `NULL` return.

use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use ::tsearch::{SpellHandle, TSLexeme};

seam_core::seam!(
    /// `NIStartBuild(&d->obj)` — begin building a fresh ISpell dictionary.
    /// Returns the opaque handle the rest of the pipeline threads.
    pub fn spell_start_build() -> PgResult<SpellHandle>
);

seam_core::seam!(
    /// `NIImportDictionary(&d->obj, filename)` — import the `.dict` file at
    /// `filename` (a NUL-free path in the database encoding).
    pub fn spell_import_dictionary(handle: SpellHandle, filename: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `NIImportAffixes(&d->obj, filename)` — import the `.affix` file.
    pub fn spell_import_affixes(handle: SpellHandle, filename: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `NISortDictionary(&d->obj)` — finalize the imported word list.
    pub fn spell_sort_dictionary(handle: SpellHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `NISortAffixes(&d->obj)` — finalize the imported affix rules.
    pub fn spell_sort_affixes(handle: SpellHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `NIFinishBuild(&d->obj)` — release the transient build context.
    pub fn spell_finish_build(handle: SpellHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `NINormalizeWord(&d->obj, txt)` — normalize the (already-lowercased)
    /// `word`, producing its stem/variant lexemes. `None` mirrors the C `NULL`
    /// return (no normalization). The `TSLexeme[]` result is allocated in
    /// `mcx` (C: the caller's current context).
    pub fn spell_normalize_word<'mcx>(
        mcx: Mcx<'mcx>,
        handle: SpellHandle,
        word: &[u8],
    ) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>>
);
