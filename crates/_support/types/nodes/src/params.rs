//! `ParamListInfo` vocabulary (`nodes/params.h`) — the parameter-list carrier
//! types owned by the `params` family of `backend-nodes-core` (`nodes/params.c`).
//!
//! These mirror the C structs field-for-field (verified against
//! `src/include/nodes/params.h`, PostgreSQL 18.3). In C a `ParamListInfoData` is
//! a variable-length palloc'd object whose flexible array member `params[]` has
//! `numParams` entries; here `params` is an owned `Vec<ParamExternData>` of that
//! length (length zero when a dynamic `paramFetch` hook is supplied).
//!
//! [`ParamListInfo`] (`Option<Rc<ParamListInfoData>>`) is the cross-crate value
//! type for a live `ParamListInfo`, shared by reference count exactly as C
//! shares its `ParamListInfoData *` by pointer — no handle, no side registry.
//!
//! ## Hooks
//!
//! C's three hook function pointers (`ParamFetchHook`, `ParamCompileHook`,
//! `ParserSetupHook`) plus their `void *` arg fields are caller-supplied
//! callbacks into other subsystems. The dynamic-fetch path is the only one the
//! params operations themselves inspect (they branch on `paramFetch != NULL`),
//! so the hooks are modeled as plain booleans / opaque arg slots here: the
//! params operations only ever need to know *whether* a fetch hook is present
//! (the static array path is the overwhelmingly common case). The compile and
//! parser-setup hooks are never invoked from `params.c` itself.

use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{Oid, ParseLoc};
use types_error::PgResult;
pub use types_tuple::heaptuple::Datum;

use crate::nodes::NodeTag;

/// `ParamListInfo` (`nodes/params.h`) — the bound-parameter list passed to a
/// query. In C this is `ParamListInfoData *`: a palloc'd, shared-by-pointer
/// value. The owned model carries it as `Option<Rc<ParamListInfoData>>` — a
/// real value shared by reference count (C's pointer aliasing), never an
/// opaque handle into a side registry. `None` is the C `NULL`.
///
/// The `'static` lifetime marks that the params live in a long-lived context
/// (`makeParamList` allocates by-reference datum images into a backend/portal
/// -lifetime `MemoryContext`, exactly as C palloc's into the per-query or
/// portal context). The struct is shared, not handle-keyed; consumers
/// (executor, plancache, pquery, prepare) read `params[id-1]` / `numParams`
/// directly off the `Rc`.
pub type ParamListInfo = Option<Rc<ParamListInfoData<'static>>>;

/// `PARAM_FLAG_CONST` (`nodes/params.h`) — the planner may treat this parameter
/// as a constant.
pub const PARAM_FLAG_CONST: u16 = 0x0001;

/// `ParamExternData` (`nodes/params.h`) — one external parameter value.
#[derive(Clone, Debug)]
pub struct ParamExternData<'mcx> {
    /// `Datum value` — parameter value.
    pub value: Datum<'mcx>,
    /// `bool isnull` — is it NULL?
    pub isnull: bool,
    /// `uint16 pflags` — flag bits, see [`PARAM_FLAG_CONST`].
    pub pflags: u16,
    /// `Oid ptype` — parameter's datatype, or `0` for an unused slot.
    pub ptype: Oid,
}

impl ParamExternData<'_> {
    /// A fresh, fully-NULL slot — the `palloc`'d initial state of each
    /// flexible-array entry and the C stack `prmdata` workspace.
    pub fn empty() -> Self {
        ParamExternData {
            value: Datum::null(),
            isnull: false,
            pflags: 0,
            ptype: 0,
        }
    }

    /// Deep copy into `mcx` (C: `copyParamList`/`datumCopy` shape — each entry's
    /// by-reference [`Datum`] is re-allocated in the target context). Fallible:
    /// copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ParamExternData<'b>> {
        Ok(ParamExternData {
            value: self.value.clone_in(mcx)?,
            isnull: self.isnull,
            pflags: self.pflags,
            ptype: self.ptype,
        })
    }
}

/// Opaque `void *paramFetchArg` / `paramCompileArg` / `parserSetupArg`
/// (`nodes/params.h`) — caller-supplied callback user-data. PostgreSQL defines
/// no concrete struct; stays a placeholder reached through `Option<Box<_>>`.
#[derive(Clone, Debug, Default)]
pub struct ParamHookArg {
    _private: (),
}

impl ParamHookArg {
    /// Deep copy into `mcx` (C: `copyObject` shape). The opaque caller user-data
    /// carries no owned children, so this is a trivial reproduction; it stays
    /// fallible to mirror the rest of the `clone_in` family.
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<ParamHookArg> {
        Ok(ParamHookArg { _private: () })
    }
}

/// `ParamListInfoData` (`nodes/params.h`) — the bound-parameter payload.
///
/// `params` is the C flexible array member: length `numParams`, or length zero
/// when `param_fetch` is supplied (the dynamic path).
#[derive(Clone, Debug)]
pub struct ParamListInfoData<'mcx> {
    /// `ParamFetchHook paramFetch` — whether a dynamic parameter-fetch hook is
    /// installed. The hook itself lives in the caller's subsystem; the params
    /// operations only branch on its presence (`paramFetch != NULL`).
    pub param_fetch: bool,
    /// `void *paramFetchArg`.
    pub param_fetch_arg: Option<Box<ParamHookArg>>,
    /// `ParamCompileHook paramCompile` — present-flag (never called from
    /// `params.c`; consumed by `execExpr.c`'s PARAM_EXTERN compilation).
    pub param_compile: bool,
    /// `void *paramCompileArg`.
    pub param_compile_arg: Option<Box<ParamHookArg>>,
    /// `ParserSetupHook parserSetup` — present-flag. C defaults this to
    /// `paramlist_parser_setup`; the parser installs the resolver explicitly in
    /// the owned model (see `nodes_core::params`).
    pub parser_setup: bool,
    /// `void *parserSetupArg`.
    pub parser_setup_arg: Option<Box<ParamHookArg>>,
    /// `char *paramValuesStr` — params as a single string for error context.
    pub param_values_str: Option<String>,
    /// `int numParams` — nominal/maximum number of params represented.
    pub num_params: i32,
    /// `ParamExternData params[FLEXIBLE_ARRAY_MEMBER]`.
    pub params: Vec<ParamExternData<'mcx>>,
}

impl ParamListInfoData<'_> {
    /// Deep copy into `mcx` (C: `copyParamList` shape — the hook flags/args and
    /// the per-parameter values are reproduced into the target context).
    /// Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ParamListInfoData<'b>> {
        let mut params = Vec::with_capacity(self.params.len());
        for p in &self.params {
            params.push(p.clone_in(mcx)?);
        }
        Ok(ParamListInfoData {
            param_fetch: self.param_fetch,
            param_fetch_arg: match &self.param_fetch_arg {
                Some(a) => Some(Box::new(a.clone_in(mcx)?)),
                None => None,
            },
            param_compile: self.param_compile,
            param_compile_arg: match &self.param_compile_arg {
                Some(a) => Some(Box::new(a.clone_in(mcx)?)),
                None => None,
            },
            parser_setup: self.parser_setup,
            parser_setup_arg: match &self.parser_setup_arg {
                Some(a) => Some(Box::new(a.clone_in(mcx)?)),
                None => None,
            },
            param_values_str: self.param_values_str.as_ref().map(|s| s.to_string()),
            num_params: self.num_params,
            params,
        })
    }
}

/// `ParamsErrorCbData` (`nodes/params.h`) — argument for `ParamsErrorCallback`.
#[derive(Clone, Debug)]
pub struct ParamsErrorCbData<'mcx> {
    /// `const char *portalName`.
    pub portal_name: Option<String>,
    /// `ParamListInfo params`.
    pub params: Option<Box<ParamListInfoData<'mcx>>>,
}

impl ParamsErrorCbData<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ParamsErrorCbData<'b>> {
        Ok(ParamsErrorCbData {
            portal_name: self.portal_name.as_ref().map(|s| s.to_string()),
            params: match &self.params {
                Some(p) => Some(Box::new(p.clone_in(mcx)?)),
                None => None,
            },
        })
    }
}

/// `T_Param` (`nodes/nodetags.h`) — node tag of a [`crate::primnodes::Param`]
/// (generated PostgreSQL 18.3 value).
pub const T_Param: NodeTag = NodeTag(8);

/// `T_ParamRef` (`nodes/nodetags.h`) — node tag of a [`ParamRef`] (generated
/// PostgreSQL 18.3 value).
pub const T_ParamRef: NodeTag = NodeTag(70);

/// `ParamRef` (`nodes/parsenodes.h`) — a `$n` parameter reference produced by
/// the grammar; transformed into a [`crate::primnodes::Param`] by
/// `paramlist_param_ref`. Not a node-support struct, so it lives here with the
/// params vocabulary rather than in `parsenodes`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ParamRef {
    /// `int number` — the number of the parameter.
    pub number: i32,
    /// `ParseLoc location` — token location, or `-1` if unknown.
    pub location: ParseLoc,
}

impl ParamRef {
    /// `makeNode(ParamRef)` with the given parameter number and location.
    pub fn new(number: i32, location: ParseLoc) -> Self {
        ParamRef { number, location }
    }

    /// Deep copy into `mcx` (C: `copyObject` shape). `ParamRef` is a flat
    /// `Copy` node with no owned children, so this is a trivial reproduction;
    /// it stays fallible to mirror the rest of the `clone_in` family.
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<ParamRef> {
        Ok(*self)
    }
}
