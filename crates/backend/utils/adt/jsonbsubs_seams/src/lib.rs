//! Per-callback seam declarations for the jsonb subscripting execution bodies
//! (`utils/adt/jsonbsubs.c`): the `SubscriptExecSteps` methods
//! `jsonb_exec_setup` installs — `jsonb_subscript_fetch`,
//! `jsonb_subscript_assign`, and `jsonb_subscript_fetch_old`.
//!
//! In C these are bare `void (*)(ExprState *, ExprEvalStep *, ExprContext *)`
//! callbacks that read the source jsonb from `*op->resvalue`, the per-subscript
//! text path from the type-specific `JsonbSubWorkspace`, and the replacement
//! value from the `SubscriptingRefState`. That raw `void` shape can not thread
//! the owned `Mcx`/result cell, so — exactly as the array subscripting bodies
//! (arraysubs.c) do — the interpreter re-dispatches a [`SubscriptMethod`]
//! discriminant with the EState threaded in and calls these per-callback seams
//! with the container value and the already-converted text path elements.
//!
//! `jsonb_subscript_check_subscripts`'s INT4→text coercion and the
//! `expectArray` determination are owner logic too, but they need no jsonb
//! primitive (only `int4out` digit formatting), so the interpreter performs
//! that conversion inline; these seams cover the three primitive-backed
//! callbacks (`jsonb_get_element` / `jsonb_set_element`).
//!
//! The owning unit (`backend-utils-adt-jsonbsubs`) installs these from its
//! `init_seams()`; until then a call panics loudly (mirror-PG-and-panic).

use mcx::Mcx;
use types_error::PgResult;
use types_tuple::heaptuple::Datum as DatumV;

seam_core::seam!(
    /// `jsonb_subscript_fetch` (jsonbsubs.c): fetch one jsonb element.
    ///
    /// The source container is in the step's result variable and is known not
    /// NULL (fetch_strict). `path` is the deconstructed text path — one
    /// `VARDATA_ANY`-payload byte string per upper subscript (the converted
    /// `workspace->index[i]`). Returns `(element, isnull)`.
    pub fn jsonb_subscript_fetch<'mcx>(
        mcx: Mcx<'mcx>,
        container: DatumV<'mcx>,
        path: &[Vec<u8>],
    ) -> PgResult<(DatumV<'mcx>, bool)>
);

seam_core::seam!(
    /// `jsonb_subscript_assign` (jsonbsubs.c): assign one jsonb element,
    /// returning the new whole jsonb value (never NULL).
    ///
    /// The input container (possibly NULL) is in the result area; a NULL input
    /// becomes an empty jsonb array (`expect_array`) or object. `replacevalue`
    /// is the new value (`replacenull` true ⇒ a jsonb `null` is stored).
    pub fn jsonb_subscript_assign<'mcx>(
        mcx: Mcx<'mcx>,
        container: DatumV<'mcx>,
        container_null: bool,
        path: &[Vec<u8>],
        replacevalue: DatumV<'mcx>,
        replacenull: bool,
        expect_array: bool,
    ) -> PgResult<(DatumV<'mcx>, bool)>
);

seam_core::seam!(
    /// `jsonb_subscript_fetch_old` (jsonbsubs.c): fetch the existing jsonb
    /// element for a nested assignment. Like the regular fetch but copes with a
    /// NULL container (returns NULL); the result is stored into the
    /// `SubscriptingRefState`'s prevvalue/prevnull by the caller.
    pub fn jsonb_subscript_fetch_old<'mcx>(
        mcx: Mcx<'mcx>,
        container: DatumV<'mcx>,
        container_null: bool,
        path: &[Vec<u8>],
    ) -> PgResult<(DatumV<'mcx>, bool)>
);
