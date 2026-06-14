//! Seam declarations for `catalog/pg_aggregate.c` (`AggregateCreate`).
//!
//! `DefineAggregate` (aggregatecmds.c) hands the fully-deconstructed CREATE
//! AGGREGATE clause bundle to `AggregateCreate`, which does the catalog-munging
//! (the `pg_proc` aggregate-implementation function insert, the `pg_aggregate`
//! row, and all the dependency recording). pg_aggregate.c is not ported yet, so
//! this seam panics until its owner lands — mirror-PG-and-panic.

use types_catalog::catalog_dependency::ObjectAddress;
use types_core::Oid;
use types_error::PgResult;
use types_parsenodes::Node;

/// The argument bundle `DefineAggregate` passes to
/// `AggregateCreate(...)` (pg_aggregate.c). Field order mirrors the C
/// parameter list. The C trades `oidvector *`/`Datum`(text[]/array) pointers;
/// the owned tree carries the natural collected forms produced by
/// `interpret_function_parameter_list` and the `defGet*` accessors.
#[derive(Clone, Debug)]
pub struct AggregateCreateArgs {
    /// aggregate name
    pub agg_name: String,
    /// namespace
    pub agg_namespace: Oid,
    pub replace: bool,
    pub agg_kind: i8,
    pub num_args: i32,
    pub num_direct_args: i32,
    /// `parameterTypes` (the `oidvector` of input arg types).
    pub parameter_types: Vec<Oid>,
    /// `allParameterTypes` (oid[] of all params incl. OUT), or `None`.
    pub all_parameter_types: Option<Vec<Oid>>,
    /// `parameterModes` ("char"[] of param modes), or `None`.
    pub parameter_modes: Option<Vec<i8>>,
    /// `parameterNames` (text[] of param names), or `None`.
    pub parameter_names: Option<Vec<Option<String>>>,
    /// `parameterDefaults` (list of default exprs) — always empty for aggregates.
    pub parameter_defaults: Vec<Node>,
    pub variadic_arg_type: Oid,
    /// step function name (qualified name list).
    pub transfunc_name: Vec<Node>,
    /// final function name.
    pub finalfunc_name: Vec<Node>,
    /// combine function name.
    pub combinefunc_name: Vec<Node>,
    /// serial function name.
    pub serialfunc_name: Vec<Node>,
    /// deserial function name.
    pub deserialfunc_name: Vec<Node>,
    /// forward moving-aggregate transition function name.
    pub mtransfunc_name: Vec<Node>,
    /// inverse moving-aggregate transition function name.
    pub minvtransfunc_name: Vec<Node>,
    /// moving-aggregate final function name.
    pub mfinalfunc_name: Vec<Node>,
    pub finalfunc_extra_args: bool,
    pub mfinalfunc_extra_args: bool,
    pub finalfunc_modify: i8,
    pub mfinalfunc_modify: i8,
    /// sort operator name (for ordered-set/hypothetical-set aggregates).
    pub sortoperator_name: Vec<Node>,
    /// transition data type
    pub trans_type_id: Oid,
    /// transition space
    pub trans_space: i32,
    /// moving-aggregate transition data type
    pub mtrans_type_id: Oid,
    /// moving-aggregate transition space
    pub mtrans_space: i32,
    /// initial condition
    pub initval: Option<String>,
    /// moving-aggregate initial condition
    pub minitval: Option<String>,
    /// parallel safety
    pub proparallel: i8,
}

seam_core::seam!(
    /// `AggregateCreate(...)` (catalog/pg_aggregate.c) — insert the
    /// implementation `pg_proc` row, the `pg_aggregate` row, and record all
    /// dependencies. Returns the aggregate's `pg_proc` object address.
    pub fn aggregate_create(args: AggregateCreateArgs) -> PgResult<ObjectAddress>
);
