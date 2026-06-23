//! The `SpellHandle` registry and seam installation.
//!
//! C embeds an `IspellDict obj` inside the caller's `DictISpell` and threads
//! `&d->obj` through the build pipeline. The
//! `backend-tsearch-spell-seams` surface keys that build state by an opaque
//! [`SpellHandle`]: `NIStartBuild` mints one, the import/sort/finish/normalize
//! steps thread it, and this module resolves it back to the real owned
//! [`IspellDict`].
//!
//! The dictionary lives for the cache lifetime, so each one is a movable
//! [`McxOwned`] bundle (its own `"Ispell dictionary"` context plus the build
//! state allocated in it) held in a backend-local table. `init_seams()`
//! installs every `backend-tsearch-spell-seams` slot.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use ::utils_error::ereport;
use mcx::{McxOwned, MemoryContext};
use types_error::{PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
use ::tsearch::SpellHandle;

use crate::IspellDict;

::mcx::bind!(IspellDictTy => IspellDict<'mcx>);

thread_local! {
    /// `SpellHandle` -> the live dictionary (C's `d->obj`, cache-lifetime).
    static SPELL_REG: RefCell<HashMap<u64, McxOwned<IspellDictTy>>> =
        RefCell::new(HashMap::new());
    /// The next token to mint (C never reuses an embedded `obj`).
    static SPELL_NEXT: Cell<u64> = const { Cell::new(1) };
}

/// Run `f` over a registered dictionary, loud on an unknown token (the C
/// analogue would be dereferencing a junk `IspellDict *`).
fn spell_with<R>(
    h: SpellHandle,
    who: &str,
    f: impl for<'mcx> FnOnce(&mut IspellDict<'mcx>) -> PgResult<R>,
) -> PgResult<R> {
    SPELL_REG.with(|m| {
        let mut m = m.borrow_mut();
        let d = m.get_mut(&h.0).ok_or_else(|| {
            ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!("spell {who}: unknown SpellHandle {}", h.0))
                .into_error()
        })?;
        d.with_mut(f)
    })
}

/// Install every seam this unit owns (`backend-tsearch-spell-seams`): the
/// ISpell build pipeline (`NIStartBuild` … `NIFinishBuild`) and
/// `NINormalizeWord`, all over the real owned [`IspellDict`].
pub fn init_seams() {
    use spell_seams as s;

    // NIStartBuild(&d->obj) — a fresh dictionary under a fresh token.
    s::spell_start_build::set(|| {
        let owned = McxOwned::<IspellDictTy>::try_new(
            MemoryContext::new("Ispell dictionary"),
            |mcx| {
                let mut d = IspellDict::new(mcx);
                d.ni_start_build()?;
                Ok(d)
            },
        )?;
        let tok = SPELL_NEXT.with(|c| {
            let t = c.get();
            c.set(t + 1);
            t
        });
        SPELL_REG.with(|m| {
            m.borrow_mut().insert(tok, owned);
        });
        Ok(SpellHandle(tok))
    });

    // NIImportDictionary(&d->obj, filename).
    s::spell_import_dictionary::set(|h, filename| {
        spell_with(h, "spell_import_dictionary", |d| d.ni_import_dictionary(filename))
    });

    // NIImportAffixes(&d->obj, filename).
    s::spell_import_affixes::set(|h, filename| {
        spell_with(h, "spell_import_affixes", |d| d.ni_import_affixes(filename))
    });

    // NISortDictionary(&d->obj).
    s::spell_sort_dictionary::set(|h| {
        spell_with(h, "spell_sort_dictionary", |d| d.ni_sort_dictionary())
    });

    // NISortAffixes(&d->obj).
    s::spell_sort_affixes::set(|h| {
        spell_with(h, "spell_sort_affixes", |d| d.ni_sort_affixes())
    });

    // NIFinishBuild(&d->obj) — release the build scratch; the dictionary itself
    // stays registered (C keeps it for the cache lifetime).
    s::spell_finish_build::set(|h| {
        spell_with(h, "spell_finish_build", |d| d.ni_finish_build())
    });

    // NINormalizeWord(&d->obj, word) — C NULL == nothing produced, so an empty
    // result maps to None (the C `lres` stays NULL when no norm is added). The
    // lexeme array is allocated in the caller's context `mcx`.
    s::spell_normalize_word::set(|mcx, h, word| {
        spell_with(h, "spell_normalize_word", |d| {
            let lexemes = d.ni_normalize_word(mcx, word)?;
            Ok(if lexemes.is_empty() {
                None
            } else {
                Some(lexemes)
            })
        })
    });
}
