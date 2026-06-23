//! Seam declarations for the shared COPY-options unit (`commands/copy.c`):
//! `ProcessCopyOptions` (parse the option list into a `CopyFormatOptions`) and
//! `CopyGetAttnums` (resolve a column-name list to physical attribute numbers).
//! Both COPY drivers (copyfrom / copyto) call these.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use ::types_copy::CopyFormatOptions;
use ::types_core::primitive::AttrNumber;
use ::types_error::PgResult;
use ::nodes::nodes::NodePtr;
use ::rel::Relation;
use ::types_tuple::heaptuple::TupleDesc;

seam_core::seam!(
    /// `ProcessCopyOptions(pstate, opts_out, is_from, options)`
    /// (commands/copy.c): parse the `DefElem` option list into a filled
    /// `CopyFormatOptions` (the C zeroes `*opts_out` then fills it; the owned
    /// model returns the filled value). `is_from` is `false` for COPY TO.
    /// `options` is the parser's `List *` of `DefElem` nodes (`None` ⇒ NIL).
    /// `Err` carries the option-validation `ereport(ERROR)`s.
    pub fn process_copy_options<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: Option<&::nodes::copy_query::ParseState<'mcx>>,
        is_from: bool,
        options: Option<&[NodePtr<'mcx>]>,
    ) -> PgResult<CopyFormatOptions<'mcx>>
);

seam_core::seam!(
    /// `CopyGetAttnums(tupDesc, rel, attnamelist)` (commands/copy.c): convert
    /// a column-name list to the integer list of physical attnums. A NIL
    /// `attnamelist` (`None`) selects all non-dropped columns in physical
    /// order. `attnamelist` is the parser's `List *` of `String` nodes. `Err`
    /// carries the "column does not exist" / "specified more than once"
    /// `ereport(ERROR)`s. The result list is allocated in `mcx`.
    pub fn copy_get_attnums<'mcx>(
        mcx: Mcx<'mcx>,
        tup_desc: &TupleDesc<'mcx>,
        rel: Option<&Relation<'mcx>>,
        attnamelist: Option<&[NodePtr<'mcx>]>,
    ) -> PgResult<PgVec<'mcx, AttrNumber>>
);
