//! Seam declarations for the `backend-parser-parse-agg` unit
//! (`parser/parse_agg.c`, part of the unported `backend-parser-medium2` unit).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly (mirror-PG-and-panic).

#![allow(non_snake_case)]

use types_nodes::nodes::Node;

seam_core::seam!(
    /// `contain_aggs_of_level(node, levelsup)` (parse_agg.c): does the node
    /// contain any aggregate of the specified query level? Infallible (a pure
    /// expression-tree walk).
    pub fn contain_aggs_of_level(node: &Node<'_>, levelsup: i32) -> bool
);

seam_core::seam!(
    /// `locate_agg_of_level(node, levelsup)` (parse_agg.c): the parse location
    /// of any aggregate of the specified query level, or `-1`. Infallible.
    pub fn locate_agg_of_level(node: &Node<'_>, levelsup: i32) -> i32
);

seam_core::seam!(
    /// `transformAggregateCall(pstate, agg, args, aggorder, agg_distinct)`
    /// (parse_agg.c): finish building an `Aggref` — wrap the (already
    /// type-coerced) plain argument expressions and any `ORDER BY` items into
    /// the aggregate's `aggdirectargs`/`args`/`aggorder`/`aggdistinct` and
    /// `aggargtypes`, set `agglevelsup`, and record the aggregate in `pstate`
    /// (`p_hasAggs`, level checks). The C mutates `*agg` and `*pstate` in place;
    /// the owned model takes the freshly-built `Aggref` plus the raw `args`
    /// (plain exprs) and `aggorder` (`SortBy` nodes) by value and returns the
    /// finished `Aggref`. `Err` carries the aggregate-placement `ereport(ERROR)`
    /// surface.
    pub fn transform_aggregate_call<'mcx>(
        pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
        agg: types_nodes::primnodes::Aggref,
        args: std::vec::Vec<types_nodes::primnodes::Expr>,
        aggorder: mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
        agg_distinct: bool,
    ) -> types_error::PgResult<types_nodes::primnodes::Aggref>
);

seam_core::seam!(
    /// `transformWindowFuncCall(pstate, wfunc, windef)` (parse_agg.c): finish
    /// building a `WindowFunc` — find or create the matching window clause in
    /// `pstate->p_windowdefs`, set `wfunc->winref`, and record the window
    /// function in `pstate` (`p_hasWindowFuncs`, level checks). The C mutates
    /// `*wfunc` and `*pstate` in place; the owned model takes the built
    /// `WindowFunc` and the `WindowDef` by value and returns the finished
    /// `WindowFunc`. `Err` carries the window-placement `ereport(ERROR)`
    /// surface.
    pub fn transform_window_func_call<'mcx>(
        pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
        wfunc: types_nodes::primnodes::WindowFunc,
        windef: types_nodes::rawnodes::WindowDef<'mcx>,
    ) -> types_error::PgResult<types_nodes::primnodes::WindowFunc>
);
