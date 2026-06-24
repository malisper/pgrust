//! Per-callback seam declarations for the hstore subscripting execution bodies
//! (`contrib/hstore/hstore_subs.c`): the `SubscriptExecSteps` methods
//! `hstore_exec_setup` installs — `hstore_subscript_fetch` and
//! `hstore_subscript_assign`.
//!
//! hstore subscripting is much simpler than array/jsonb: only a single,
//! non-slice, text subscript, and the fetch result is always a plain `text`
//! (never a container), so there is no `sbs_check_subscripts` and no
//! `sbs_fetch_old` (hstore_subs.c installs only `sbs_fetch`/`sbs_assign`).
//!
//! In C these are bare `void (*)(ExprState *, ExprEvalStep *, ExprContext *)`
//! callbacks that read the source hstore from `*op->resvalue` and the single
//! `text` subscript from `sbsrefstate->upperindex[0]`. That raw `void` shape can
//! not thread the owned `Mcx`/result cell, so — exactly as the array/jsonb
//! subscripting bodies do — the interpreter re-dispatches a `SubscriptMethod`
//! discriminant with the EState threaded in and calls these per-callback seams
//! with the container value and the already-evaluated `text` subscript.
//!
//! The owning unit (`hstore`) installs these from its `init_seams()`; until then
//! a call panics loudly (mirror-PG-and-panic).

use ::mcx::Mcx;
use ::types_error::PgResult;
use types_tuple::heaptuple::Datum as DatumV;

seam_core::seam!(
    /// `hstore_subscript_fetch` (hstore_subs.c): fetch the `text` value for one
    /// hstore key.
    ///
    /// The source container is in the step's result variable and is known not
    /// NULL (fetch_strict). `key` is the `text` subscript Datum
    /// (`sbsrefstate->upperindex[0]`); it is known not NULL here (a NULL
    /// subscript short-circuited the result to NULL before this seam is
    /// reached). Returns `(text_value, isnull)` — `isnull` true when the key is
    /// absent or its value is SQL NULL (cf. `hstore_fetchval`).
    pub fn hstore_subscript_fetch<'mcx>(
        mcx: Mcx<'mcx>,
        container: DatumV<'mcx>,
        key: DatumV<'mcx>,
    ) -> PgResult<(DatumV<'mcx>, bool)>
);

seam_core::seam!(
    /// `hstore_subscript_assign` (hstore_subs.c): set/replace one hstore key,
    /// returning the new whole hstore value (never NULL).
    ///
    /// The input container (possibly NULL) is in the result area; a NULL input
    /// becomes a one-element hstore. `key` is the `text` subscript Datum (known
    /// not NULL — a NULL assignment subscript errors before this seam).
    /// `replacevalue` is the new value (`replacenull` true ⇒ the key's value is
    /// stored as SQL NULL).
    pub fn hstore_subscript_assign<'mcx>(
        mcx: Mcx<'mcx>,
        container: DatumV<'mcx>,
        container_null: bool,
        key: DatumV<'mcx>,
        replacevalue: DatumV<'mcx>,
        replacenull: bool,
    ) -> PgResult<(DatumV<'mcx>, bool)>
);
