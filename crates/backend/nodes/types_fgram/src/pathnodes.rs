//! Planner / extensible node types (`nodes/pathnodes.h`, `nodes/extensible.h`,
//! plus `utils/rel.h`'s `ForeignKeyCacheInfo`).
//!
//! Most of `pathnodes.h` is `pg_node_attr(no_copy_equal)` planner-internal
//! state that `copyObject`/`equal` never traverse (the `Path` / `RelOptInfo`
//! family, `EquivalenceClass`, `EquivalenceMember`, ...). This module models
//! *only* the handful of pathnode structs that copyfuncs **or** equalfuncs do
//! handle - i.e. those NOT marked `no_copy` and `no_equal`:
//!
//! - [`PathKey`], [`GroupByOrdering`] (copy + equal),
//! - [`RestrictInfo`] (copy + equal),
//! - [`PlaceHolderVar`] (copy + equal),
//! - [`SpecialJoinInfo`] (copy + equal),
//! - [`PlaceHolderInfo`] (copy + equal),
//! - [`ForeignKeyCacheInfo`] (`no_equal`, so copy only),
//! - [`ExtensibleNode`] (`custom_copy_equal`: copy/equal go through the
//!   extension-registered [`ExtensibleNodeMethods`]).
//!
//! `AppendRelInfo` is also copy/equal-supported but lives (with the rest of the
//! parse-tree-adjacent nodes) in [`crate::parsenodes`]; it is not redefined
//! here.
//!
//! Layout is faithful to the C backend: every struct is `#[repr(C)]` with field
//! order, names, types, and widths matching the headers, cross-checked against
//! the c2rust-emitted struct defs of the generated copy/equal functions. Node
//! trees are `palloc`/`MemoryContext`-owned, so there is no `Box`/`Drop` and no
//! `extern "C"`.
//!
//! # `pg_node_attr` honoring
//!
//! The copy/equal *logic* lives in the copyfuncs/equalfuncs crates, but the
//! attributes that shape it are documented per field here so the layer stays
//! faithful:
//!
//! - `copy_as_scalar` (e.g. `RestrictInfo.parent_ec`, `PathKey.pk_eclass`): the
//!   pointee is a planner-internal struct that copy/equal never deep-copy or
//!   compare. We type those fields as raw pointers to the [`OpaqueNode`]-style
//!   forward declarations [`EquivalenceClass`], [`EquivalenceMember`], and
//!   [`MergeScanSelCache`]. The field still occupies one pointer slot, matching
//!   the C ABI; copy assigns the pointer value as-is.
//! - `copy_as(NIL)` (`RestrictInfo.scansel_cache`): copy resets the field to
//!   `NIL` (null) rather than deep-copying. Modelled as a `*mut List` (the C
//!   type) so the slot width matches; copy stores null.
//! - `equal_ignore`: the field is skipped by `equal`. Still modelled with its
//!   real type/width so the struct layout is exact.
//! - `array_size(nkeys)` (`ForeignKeyCacheInfo`): fixed-size `[T; INDEX_MAX_KEYS]`
//!   array; copy `memcpy`s the whole fixed array.

use core::ffi::{c_char, c_double, c_int};

use pg_ffi_fgram::{AttrNumber, Bitmapset, CompareType, List, NodeTag, Oid, Size};

use crate::primnodes::{Cost, Expr, Index, JoinType};

// ---------------------------------------------------------------------------
// Embedded-by-value planner types, modelled with exact layout.
// ---------------------------------------------------------------------------

/// `int32` as PostgreSQL spells it (used for `PlaceHolderInfo.ph_width`).
pub type int32 = i32;

/// `Selectivity` (`nodes/nodes.h`) - fraction of tuples a qualifier passes.
/// A bare `double`.
pub type Selectivity = c_double;

/// `Cardinality` (`nodes/nodes.h`) - estimated number of rows. A bare `double`.
/// (Re-exported alongside `Cost` for callers of this module; `Cost` itself is
/// reused from [`crate::primnodes`].)
pub type Cardinality = c_double;

/// `Relids` (`nodes/pathnodes.h`) - `typedef Bitmapset *Relids`. A relid set is
/// deep-copied with `bms_copy` and compared with `bms_equal`.
pub type Relids = *mut Bitmapset;

/// `VolatileFunctionStatus` (`nodes/pathnodes.h`) - cached
/// `contain_volatile_functions` property. Discriminants are exact.
pub type VolatileFunctionStatus = core::ffi::c_uint;
pub const VOLATILITY_UNKNOWN: VolatileFunctionStatus = 0;
pub const VOLATILITY_VOLATILE: VolatileFunctionStatus = 1;
pub const VOLATILITY_NOVOLATILE: VolatileFunctionStatus = 2;

/// `QualCost` (`nodes/pathnodes.h`) - one-time (startup) plus per-tuple cost.
/// Embedded by value in [`RestrictInfo`]; modelled with exact layout (two
/// `Cost` = two `f64`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QualCost {
    /// one-time cost
    pub startup: Cost,
    /// per-evaluation cost
    pub per_tuple: Cost,
}

/// `INDEX_MAX_KEYS` (`pg_config_manual.h`) - the fixed bound on the
/// `array_size(nkeys)` arrays in [`ForeignKeyCacheInfo`].
pub const INDEX_MAX_KEYS: usize = 32;

// ---------------------------------------------------------------------------
// Opaque forward declarations for `copy_as_scalar` pointee types.
//
// These planner-internal structs are referenced only through `copy_as_scalar`
// (and, where shown, `equal_ignore` / `equal_as_scalar`) fields: copy assigns
// the pointer value as-is and equal never dereferences it. They are therefore
// never deep-copied or compared, so we model them as pointer-sized,
// `NodeTag`-headed forward declarations rather than committing to their full
// (large, planner-only) layout. This is exactly the documented [`OpaqueNode`]
// seam, specialized to the three pointee types these fields use.
// ---------------------------------------------------------------------------

/// Opaque forward declaration for `EquivalenceClass` (`copy_as_scalar`
/// pointee). Never deep-copied or compared; only its pointer value is carried.
#[repr(C)]
pub struct EquivalenceClass {
    pub type_: NodeTag,
    _opaque: [u8; 0],
}

/// Opaque forward declaration for `EquivalenceMember` (`copy_as_scalar`
/// pointee). Never deep-copied or compared.
#[repr(C)]
pub struct EquivalenceMember {
    pub type_: NodeTag,
    _opaque: [u8; 0],
}

/// Opaque forward declaration for `MergeScanSelCache` (the pointee element type
/// of `RestrictInfo.scansel_cache`). These are not Nodes; the list is
/// `copy_as(NIL)` (reset to `NIL` on copy) and `equal_ignore`, so the pointee
/// is never deep-copied or compared and is modelled opaquely.
#[repr(C)]
pub struct MergeScanSelCache {
    _opaque: [u8; 0],
}

// ---------------------------------------------------------------------------
// PathKey / GroupByOrdering.
// ---------------------------------------------------------------------------

/// `PathKey` (`pg_node_attr(no_read, no_query_jumble)`) - one component of a
/// path's sort ordering. copy + equal supported.
///
/// `pk_eclass` is `pg_node_attr(copy_as_scalar, equal_as_scalar)`: copy carries
/// the pointer as-is and equal compares it by pointer identity (the
/// [`EquivalenceClass`] is interned per planner run), so the pointee is never
/// traversed.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PathKey {
    pub type_: NodeTag,
    /// the value that is ordered (copy_as_scalar, equal_as_scalar)
    pub pk_eclass: *mut EquivalenceClass,
    /// index opfamily defining the ordering
    pub pk_opfamily: Oid,
    /// sort direction (ASC or DESC)
    pub pk_cmptype: CompareType,
    /// do NULLs come before normal values?
    pub pk_nulls_first: bool,
}

/// `GroupByOrdering` - an order of group-by clauses plus the corresponding
/// pathkeys. copy + equal supported; both members are deep `List`s.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GroupByOrdering {
    pub type_: NodeTag,
    pub pathkeys: *mut List,
    pub clauses: *mut List,
}

// ---------------------------------------------------------------------------
// RestrictInfo.
// ---------------------------------------------------------------------------

/// `RestrictInfo` (`pg_node_attr(no_read, no_query_jumble)`) - a represented
/// WHERE/JOIN clause plus planner caches. copy + equal supported.
///
/// Per-field `pg_node_attr` honoring (driving the copy/equal layers):
/// - `can_join`, `pseudoconstant`, `leakproof`, `has_volatile`,
///   `num_base_rels`, `clause_relids`, `left_relids`, `right_relids`,
///   `orclause`, `parent_ec`, `eval_cost`, `norm_selec`, `outer_selec`,
///   `mergeopfamilies`, `left_ec`/`right_ec`/`left_em`/`right_em`,
///   `scansel_cache`, `outer_is_left`, `hashjoinoperator`,
///   `left_bucketsize`/`right_bucketsize`/`left_mcvfreq`/`right_mcvfreq`,
///   `left_hasheqoperator`/`right_hasheqoperator`, `clause_relids` are all
///   `equal_ignore` (copied but skipped by `equal`).
/// - `parent_ec`, `left_ec`, `right_ec`, `left_em`, `right_em` are
///   `copy_as_scalar` (pointer carried as-is; modelled as opaque pointees).
/// - `scansel_cache` is `copy_as(NIL)` (reset to `NIL`/null on copy).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RestrictInfo {
    pub type_: NodeTag,
    /// the represented clause of WHERE or JOIN (deep-copied/compared)
    pub clause: *mut Expr,
    /// true if clause was pushed down in level
    pub is_pushed_down: bool,
    /// equal_ignore
    pub can_join: bool,
    /// equal_ignore
    pub pseudoconstant: bool,
    pub has_clone: bool,
    pub is_clone: bool,
    /// true if known to contain no leaked Vars (equal_ignore)
    pub leakproof: bool,
    /// indicates if clause contains volatile functions (equal_ignore)
    pub has_volatile: VolatileFunctionStatus,
    pub security_level: Index,
    /// number of base rels in clause_relids (equal_ignore)
    pub num_base_rels: c_int,
    /// relids referenced in the clause (equal_ignore)
    pub clause_relids: Relids,
    /// relids required to evaluate the clause
    pub required_relids: Relids,
    /// relids above which we cannot evaluate the clause
    pub incompatible_relids: Relids,
    /// outer-side relations of an outer-join clause, else NULL
    pub outer_relids: Relids,
    /// relids in the left side (equal_ignore)
    pub left_relids: Relids,
    /// relids in the right side (equal_ignore)
    pub right_relids: Relids,
    /// modified OR clause with RestrictInfos, else NULL (equal_ignore)
    pub orclause: *mut Expr,
    /// serial number of this RestrictInfo
    pub rinfo_serial: c_int,
    /// generating EquivalenceClass (copy_as_scalar, equal_ignore,
    /// read_write_ignore)
    pub parent_ec: *mut EquivalenceClass,
    /// eval cost of clause; -1 if not yet set (equal_ignore)
    pub eval_cost: QualCost,
    /// JOIN_INNER selectivity; -1 if not set (equal_ignore)
    pub norm_selec: Selectivity,
    /// outer join selectivity; -1 if not set (equal_ignore)
    pub outer_selec: Selectivity,
    /// opfamilies containing clause operator, else NIL (equal_ignore)
    pub mergeopfamilies: *mut List,
    /// EquivalenceClass containing lefthand (copy_as_scalar, equal_ignore)
    pub left_ec: *mut EquivalenceClass,
    /// EquivalenceClass containing righthand (copy_as_scalar, equal_ignore)
    pub right_ec: *mut EquivalenceClass,
    /// EquivalenceMember for lefthand (copy_as_scalar, equal_ignore)
    pub left_em: *mut EquivalenceMember,
    /// EquivalenceMember for righthand (copy_as_scalar, equal_ignore)
    pub right_em: *mut EquivalenceMember,
    /// list of MergeScanSelCache structs (copy_as(NIL), equal_ignore)
    pub scansel_cache: *mut List,
    /// transient workspace (equal_ignore)
    pub outer_is_left: bool,
    /// copy of clause operator if hashjoinable, else InvalidOid (equal_ignore)
    pub hashjoinoperator: Oid,
    /// avg bucketsize of left side (equal_ignore)
    pub left_bucketsize: Selectivity,
    /// avg bucketsize of right side (equal_ignore)
    pub right_bucketsize: Selectivity,
    /// left side's most common val's freq (equal_ignore)
    pub left_mcvfreq: Selectivity,
    /// right side's most common val's freq (equal_ignore)
    pub right_mcvfreq: Selectivity,
    /// hash equality operator for memoize, else InvalidOid (equal_ignore)
    pub left_hasheqoperator: Oid,
    /// hash equality operator for memoize, else InvalidOid (equal_ignore)
    pub right_hasheqoperator: Oid,
}

// ---------------------------------------------------------------------------
// PlaceHolderVar.
// ---------------------------------------------------------------------------

/// `PlaceHolderVar` (`pg_node_attr(no_query_jumble)`) - placeholder for an
/// expression evaluated below the top of the plan tree. An `Expr` subtype
/// (begins with `Expr xpr`). copy + equal supported.
///
/// `phexpr` and `phrels` are `equal_ignore` (copied but not compared); only
/// `phnullingrels`, `phid`, `phlevelsup` participate in `equal`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PlaceHolderVar {
    pub xpr: Expr,
    /// the represented expression (equal_ignore)
    pub phexpr: *mut Expr,
    /// base+OJ relids syntactically within expr src (equal_ignore)
    pub phrels: Relids,
    /// RT indexes of outer joins that can null PHV's value
    pub phnullingrels: Relids,
    /// ID for PHV (unique within planner run)
    pub phid: Index,
    /// > 0 if PHV belongs to outer query
    pub phlevelsup: Index,
}

// ---------------------------------------------------------------------------
// SpecialJoinInfo.
// ---------------------------------------------------------------------------

/// `SpecialJoinInfo` (`pg_node_attr(no_read, no_query_jumble)`) - info about a
/// non-inner join recorded for join-order planning. copy + equal supported; all
/// fields are compared.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SpecialJoinInfo {
    pub type_: NodeTag,
    /// base+OJ relids in minimum LHS for join
    pub min_lefthand: Relids,
    /// base+OJ relids in minimum RHS for join
    pub min_righthand: Relids,
    /// base+OJ relids syntactically within LHS
    pub syn_lefthand: Relids,
    /// base+OJ relids syntactically within RHS
    pub syn_righthand: Relids,
    /// always INNER, LEFT, FULL, SEMI, or ANTI
    pub jointype: JoinType,
    /// outer join's RT index; 0 if none
    pub ojrelid: Index,
    /// commuting OJs above this one, if LHS
    pub commute_above_l: Relids,
    /// commuting OJs above this one, if RHS
    pub commute_above_r: Relids,
    /// commuting OJs in this one's LHS
    pub commute_below_l: Relids,
    /// commuting OJs in this one's RHS
    pub commute_below_r: Relids,
    /// joinclause is strict for some LHS rel
    pub lhs_strict: bool,
    /// true if semi_operators are all btree
    pub semi_can_btree: bool,
    /// true if semi_operators are all hash
    pub semi_can_hash: bool,
    /// OIDs of equality join operators
    pub semi_operators: *mut List,
    /// righthand-side expressions of these ops
    pub semi_rhs_exprs: *mut List,
}

// ---------------------------------------------------------------------------
// PlaceHolderInfo.
// ---------------------------------------------------------------------------

/// `PlaceHolderInfo` (`pg_node_attr(no_read, no_query_jumble)`) - per-PHV
/// planner bookkeeping. copy + equal supported; all fields are compared.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PlaceHolderInfo {
    pub type_: NodeTag,
    /// ID for PH (unique within planner run)
    pub phid: Index,
    /// copy of PlaceHolderVar tree
    pub ph_var: *mut PlaceHolderVar,
    /// lowest level we can evaluate value at
    pub ph_eval_at: Relids,
    /// relids of contained lateral refs, if any
    pub ph_lateral: Relids,
    /// highest level the value is needed at
    pub ph_needed: Relids,
    /// estimated attribute width
    pub ph_width: int32,
}

// ---------------------------------------------------------------------------
// IndexClause.
// ---------------------------------------------------------------------------

/// `IndexClause` (`pg_node_attr(no_copy_equal, no_read, no_query_jumble)`) - the
/// derivation of one index qualification clause from an original restriction or
/// join clause. Not copy/equal/read-supported (hence not modelled by the
/// copy/equal layers), but it is a real `Node` that
/// `expression_tree_walker`/`expression_tree_mutator` descend into, so its exact
/// layout is modelled here.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexClause {
    pub type_: NodeTag,
    /// original restriction or join clause
    pub rinfo: *mut RestrictInfo,
    /// indexqual(s) derived from it
    pub indexquals: *mut List,
    /// are indexquals a lossy version of clause?
    pub lossy: bool,
    /// index column the clause uses (zero-based)
    pub indexcol: AttrNumber,
    /// multiple index columns, if RowCompare
    pub indexcols: *mut List,
}

// ---------------------------------------------------------------------------
// ForeignKeyCacheInfo (utils/rel.h).
// ---------------------------------------------------------------------------

/// `ForeignKeyCacheInfo` (`utils/rel.h`, `pg_node_attr(no_equal, no_read,
/// no_query_jumble)`) - cached foreign-key constraint info. copy only (it is
/// `no_equal`).
///
/// The three trailing arrays are `pg_node_attr(array_size(nkeys))`: only the
/// first `nkeys` entries are semantically valid, but the storage is a fixed
/// `[T; INDEX_MAX_KEYS]` and copyfuncs `memcpy`s the entire fixed array.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForeignKeyCacheInfo {
    pub type_: NodeTag,
    /// oid of the constraint itself
    pub conoid: Oid,
    /// relation constrained by the foreign key
    pub conrelid: Oid,
    /// relation referenced by the foreign key
    pub confrelid: Oid,
    /// number of columns in the foreign key
    pub nkeys: c_int,
    /// is enforced?
    pub conenforced: bool,
    /// cols in referencing table (array_size(nkeys))
    pub conkey: [AttrNumber; INDEX_MAX_KEYS],
    /// cols in referenced table (array_size(nkeys))
    pub confkey: [AttrNumber; INDEX_MAX_KEYS],
    /// PK = FK operator OIDs (array_size(nkeys))
    pub conpfeqop: [Oid; INDEX_MAX_KEYS],
}

// ---------------------------------------------------------------------------
// ExtensibleNode (nodes/extensible.h) + the registry seam.
// ---------------------------------------------------------------------------

/// `ExtensibleNode` (`pg_node_attr(custom_copy_equal, custom_read_write)`) - an
/// extension-defined node. The node tag is always `T_ExtensibleNode`;
/// `extnodename` identifies the concrete type, which is looked up to find its
/// [`ExtensibleNodeMethods`].
///
/// Copy/equal do NOT have field-by-field logic for the private fields: they
/// dispatch through the registered methods (`nodeCopy` / `nodeEqual`). The
/// public header here is exactly the two leading fields the core system itself
/// copies/compares (`type`, `extnodename`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExtensibleNode {
    pub type_: NodeTag,
    /// identifier of ExtensibleNodeMethods (the lookup key)
    pub extnodename: *const c_char,
}

/// `ExtensibleNodeMethods` (`nodes/extensible.h`) - the per-extnodename method
/// table. `copyObject(ExtensibleNode)` allocates `node_size` bytes and calls
/// `nodeCopy`; `equal(ExtensibleNode)` calls `nodeEqual` after the core system
/// has matched `extnodename`.
///
/// The callbacks are modelled as raw `extern`-free function pointers
/// (`Option<unsafe fn(...)>`) so the table layout matches C. `nodeOut` /
/// `nodeRead` belong to outfuncs/readfuncs and are not exercised by the
/// copy/equal layer, but are present for an exact ABI.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExtensibleNodeMethods {
    pub extnodename: *const c_char,
    /// size of an extensible node of this type, in bytes
    pub node_size: Size,
    /// deep-copies private fields from `oldnode` to `newnode`
    pub nodeCopy: Option<unsafe fn(newnode: *mut ExtensibleNode, oldnode: *const ExtensibleNode)>,
    /// deep equality comparison of private fields
    pub nodeEqual: Option<unsafe fn(a: *const ExtensibleNode, b: *const ExtensibleNode) -> bool>,
    /// serialization (outfuncs); not used by copy/equal
    pub nodeOut: Option<unsafe fn(str: *mut core::ffi::c_void, node: *const ExtensibleNode)>,
    /// deserialization (readfuncs); not used by copy/equal
    pub nodeRead: Option<unsafe fn(node: *mut ExtensibleNode)>,
}

// ---------------------------------------------------------------------------
// Unified, process-global extensible-node registry seam.
//
// In the C backend there is exactly ONE registry of extensible-node methods: the
// process-global `extensible_node_methods` hash table (nodes/extensible.c),
// populated once by `RegisterExtensibleNodeMethods(methods)` and consulted by
// BOTH `_copyExtensibleNode` and `_equalExtensibleNode` (and outfuncs/readfuncs)
// via `GetExtensibleNodeMethods(extnodename, missing_ok)`. Because every consumer
// reads the same table, a registered extension's `nodeCopy` and `nodeEqual` stay
// consistent: copy never raises "not registered" while equal resolves it.
//
// To preserve that single-source-of-truth here, the resolver lives in THIS
// (shared) crate, and both the copyfuncs and equalfuncs ports consult it through
// [`get_extensible_node_methods`]. The seam is a single process-global function
// pointer (the safe analogue of the C hash table + `RegisterExtensibleNodeMethods`):
// a host that loads extensions installs one resolver, and from then on copy and
// equal dispatch identically.
//
// It is held in an `AtomicPtr` rather than a thread-local so that it is
// process-wide, exactly like C's `extensible_node_methods` (which is shared by
// every backend thread of control, not per-thread), and so this `#![no_std]`
// crate needs no `std` thread-local machinery.
// ---------------------------------------------------------------------------

use core::sync::atomic::{AtomicPtr, Ordering};

/// Resolver for extensible-node method lookup: the safe analogue of the C
/// `extensible_node_methods` hash table's lookup. Returns the full
/// [`ExtensibleNodeMethods`] table registered for `name` (giving BOTH `nodeCopy`
/// and `nodeEqual`, so copy and equal stay consistent), or `None` when nothing is
/// registered for that name (the `missing_ok = true` outcome).
///
/// # Safety
/// `name` is a non-null, NUL-terminated C string valid for the call; the returned
/// reference, if any, must be `'static` (the C method tables are
/// process-lifetime).
pub type ExtensibleNodeMethodsResolver =
    unsafe fn(name: *const c_char) -> Option<&'static ExtensibleNodeMethods>;

/// The single process-global extensible-node resolver, analogous to the C
/// `extensible_node_methods` hash table. `null` means "nothing registered"
/// (fail-safe default): both copy and equal then raise the C
/// `ExtensibleNodeMethods "%s" was not registered` error for any
/// `ExtensibleNode`. A host wires real extensions via
/// [`set_extensible_node_methods_resolver`].
static EXTENSIBLE_NODE_METHODS_RESOLVER: AtomicPtr<()> = AtomicPtr::new(core::ptr::null_mut());

/// Install the process-global extensible-node methods resolver, the unified seam
/// that BOTH `copyObject(ExtensibleNode)` and `equal(ExtensibleNode)` consult --
/// the safe analogue of `RegisterExtensibleNodeMethods` populating the single C
/// hash table. Because copy and equal read this one resolver, a registered
/// extension's `nodeCopy`/`nodeEqual` apply identically to both (no asymmetry
/// where equal resolves a node but copy still raises "not registered").
///
/// Returns the previously installed resolver, so callers can restore it (e.g. in
/// tests). Passing `None` reverts to the fail-safe "nothing registered" default.
pub fn set_extensible_node_methods_resolver(
    resolver: Option<ExtensibleNodeMethodsResolver>,
) -> Option<ExtensibleNodeMethodsResolver> {
    // Function-pointer <-> data-pointer round-trip: `fn` items are
    // pointer-sized and, on every platform this backend targets, share the data
    // pointer representation, so storing them in an `AtomicPtr<()>` is sound.
    let new = match resolver {
        Some(f) => f as *mut (),
        None => core::ptr::null_mut(),
    };
    let prev = EXTENSIBLE_NODE_METHODS_RESOLVER.swap(new, Ordering::SeqCst);
    if prev.is_null() {
        None
    } else {
        // Safety: `prev` was produced by a prior `set_*` call from a valid
        // `ExtensibleNodeMethodsResolver` (the only writer of this atomic).
        Some(unsafe { core::mem::transmute::<*mut (), ExtensibleNodeMethodsResolver>(prev) })
    }
}

/// Read the currently installed resolver, if any (the analogue of "the
/// `extensible_node_methods` table exists").
fn extensible_node_methods_resolver() -> Option<ExtensibleNodeMethodsResolver> {
    let p = EXTENSIBLE_NODE_METHODS_RESOLVER.load(Ordering::SeqCst);
    if p.is_null() {
        None
    } else {
        // Safety: `p` was stored from a valid resolver by `set_*`.
        Some(unsafe { core::mem::transmute::<*mut (), ExtensibleNodeMethodsResolver>(p) })
    }
}

/// Safe analogue of C's `GetExtensibleNodeMethods(extnodename, missing_ok)`
/// (nodes/extensible.c). Looks `name` up in the single process-global resolver
/// and returns its [`ExtensibleNodeMethods`] table -- the SAME table both copy and
/// equal use, so they stay consistent for every registered extension.
///
/// Returns `Ok(Some(methods))` when a table is registered for `name`, `Ok(None)`
/// when none is and `missing_ok` is `true`, and the C error
/// `ereport(ERROR, ERRCODE_UNDEFINED_OBJECT, "ExtensibleNodeMethods \"%s\" was
/// not registered")` when none is and `missing_ok` is `false`.
///
/// # Safety
/// `name` must be a non-null, NUL-terminated C string valid for the call.
pub unsafe fn get_extensible_node_methods(
    name: *const c_char,
    missing_ok: bool,
) -> ExtensibleNodeMethodsResult {
    let methods = match extensible_node_methods_resolver() {
        // Safety: `name` is a valid C string per this function's contract.
        Some(resolve) => unsafe { resolve(name) },
        None => None,
    };
    match methods {
        Some(m) => ExtensibleNodeMethodsResult::Found(m),
        None if missing_ok => ExtensibleNodeMethodsResult::NotFound,
        None => ExtensibleNodeMethodsResult::Unregistered,
    }
}

/// Outcome of [`get_extensible_node_methods`], mirroring the three states of C's
/// `GetExtensibleNodeMethods`: found, not-found-but-`missing_ok`, and the
/// not-registered error site. Kept `#![no_std]`-friendly (no `PgError` in this
/// types crate); the copy/equal layers translate [`Unregistered`] into the exact
/// `ereport`.
///
/// [`Unregistered`]: ExtensibleNodeMethodsResult::Unregistered
pub enum ExtensibleNodeMethodsResult {
    /// A method table is registered for the name.
    Found(&'static ExtensibleNodeMethods),
    /// No table is registered and the caller passed `missing_ok = true`.
    NotFound,
    /// No table is registered and the caller passed `missing_ok = false`; the
    /// caller must raise the C `ERRCODE_UNDEFINED_OBJECT` "was not registered"
    /// error.
    Unregistered,
}

// ---------------------------------------------------------------------------
// Layout asserts (faithful ABI).
// ---------------------------------------------------------------------------

const _: () = {
    use core::mem::{align_of, offset_of, size_of};

    // QualCost is exactly two f64s, no padding.
    assert!(size_of::<QualCost>() == 2 * size_of::<f64>());
    assert!(offset_of!(QualCost, startup) == 0);
    assert!(offset_of!(QualCost, per_tuple) == size_of::<f64>());

    // copy_as_scalar pointees and the NIL'd list are all one pointer wide.
    assert!(size_of::<*mut EquivalenceClass>() == size_of::<usize>());
    assert!(size_of::<*mut EquivalenceMember>() == size_of::<usize>());
    assert!(size_of::<*mut MergeScanSelCache>() == size_of::<usize>());
    assert!(size_of::<Relids>() == size_of::<usize>());

    // PathKey: NodeTag(4) + pad(4) + ptr(8) + Oid(4) + CompareType(4) + bool(1)
    // -> 8-aligned size 32.
    assert!(offset_of!(PathKey, type_) == 0);
    assert!(offset_of!(PathKey, pk_eclass) == 8);
    assert!(offset_of!(PathKey, pk_opfamily) == 16);
    assert!(offset_of!(PathKey, pk_cmptype) == 20);
    assert!(offset_of!(PathKey, pk_nulls_first) == 24);
    assert!(size_of::<PathKey>() == 32);
    assert!(align_of::<PathKey>() == 8);

    // GroupByOrdering: NodeTag(4)+pad(4)+ptr(8)+ptr(8) = 24.
    assert!(offset_of!(GroupByOrdering, pathkeys) == 8);
    assert!(offset_of!(GroupByOrdering, clauses) == 16);
    assert!(size_of::<GroupByOrdering>() == 24);

    // PlaceHolderVar begins with the Expr header at offset 0 (Expr subtype).
    assert!(offset_of!(PlaceHolderVar, xpr) == 0);
    assert!(offset_of!(PlaceHolderVar, phexpr) == 8);
    assert!(offset_of!(PlaceHolderVar, phrels) == 16);
    assert!(offset_of!(PlaceHolderVar, phnullingrels) == 24);
    assert!(offset_of!(PlaceHolderVar, phid) == 32);
    assert!(offset_of!(PlaceHolderVar, phlevelsup) == 36);
    assert!(size_of::<PlaceHolderVar>() == 40);

    // SpecialJoinInfo: NodeTag(4)+pad(4) then 4 ptrs, JoinType(4)+Index(4),
    // 4 ptrs, 3 bools+pad, 2 ptrs.
    assert!(offset_of!(SpecialJoinInfo, min_lefthand) == 8);
    assert!(offset_of!(SpecialJoinInfo, jointype) == 40);
    assert!(offset_of!(SpecialJoinInfo, ojrelid) == 44);
    assert!(offset_of!(SpecialJoinInfo, commute_above_l) == 48);
    assert!(offset_of!(SpecialJoinInfo, lhs_strict) == 80);
    assert!(offset_of!(SpecialJoinInfo, semi_operators) == 88);
    assert!(offset_of!(SpecialJoinInfo, semi_rhs_exprs) == 96);
    assert!(size_of::<SpecialJoinInfo>() == 104);

    // PlaceHolderInfo: NodeTag(4)+Index(4)+ptr(8)+3*ptr(24)+int32(4)+pad(4).
    assert!(offset_of!(PlaceHolderInfo, phid) == 4);
    assert!(offset_of!(PlaceHolderInfo, ph_var) == 8);
    assert!(offset_of!(PlaceHolderInfo, ph_eval_at) == 16);
    assert!(offset_of!(PlaceHolderInfo, ph_needed) == 32);
    assert!(offset_of!(PlaceHolderInfo, ph_width) == 40);
    assert!(size_of::<PlaceHolderInfo>() == 48);

    // RestrictInfo: pin the header, the QualCost-by-value region, and the size.
    assert!(offset_of!(RestrictInfo, clause) == 8);
    assert!(offset_of!(RestrictInfo, is_pushed_down) == 16);
    assert!(offset_of!(RestrictInfo, has_volatile) == 24);
    assert!(offset_of!(RestrictInfo, security_level) == 28);
    assert!(offset_of!(RestrictInfo, num_base_rels) == 32);
    assert!(offset_of!(RestrictInfo, clause_relids) == 40);
    assert!(offset_of!(RestrictInfo, orclause) == 88);
    assert!(offset_of!(RestrictInfo, rinfo_serial) == 96);
    assert!(offset_of!(RestrictInfo, parent_ec) == 104);
    assert!(offset_of!(RestrictInfo, eval_cost) == 112);
    assert!(offset_of!(RestrictInfo, norm_selec) == 128);
    assert!(offset_of!(RestrictInfo, outer_selec) == 136);
    assert!(offset_of!(RestrictInfo, mergeopfamilies) == 144);
    assert!(offset_of!(RestrictInfo, scansel_cache) == 184);
    assert!(offset_of!(RestrictInfo, outer_is_left) == 192);
    assert!(offset_of!(RestrictInfo, hashjoinoperator) == 196);
    assert!(offset_of!(RestrictInfo, left_bucketsize) == 200);
    assert!(offset_of!(RestrictInfo, left_hasheqoperator) == 232);
    assert!(offset_of!(RestrictInfo, right_hasheqoperator) == 236);
    assert!(size_of::<RestrictInfo>() == 240);

    // ForeignKeyCacheInfo: header, scalar fields, then the three fixed arrays.
    assert!(offset_of!(ForeignKeyCacheInfo, conoid) == 4);
    assert!(offset_of!(ForeignKeyCacheInfo, nkeys) == 16);
    assert!(offset_of!(ForeignKeyCacheInfo, conenforced) == 20);
    assert!(offset_of!(ForeignKeyCacheInfo, conkey) == 22);
    assert!(offset_of!(ForeignKeyCacheInfo, confkey) == 22 + 2 * INDEX_MAX_KEYS);
    // conpfeqop is `Oid[32]` (4-aligned) after two `AttrNumber[32]` (i16).
    assert!(offset_of!(ForeignKeyCacheInfo, conpfeqop) == 152);
    assert!(size_of::<ForeignKeyCacheInfo>() == 280);

    // ExtensibleNode header: NodeTag + one pointer.
    assert!(offset_of!(ExtensibleNode, type_) == 0);
    assert!(offset_of!(ExtensibleNode, extnodename) == 8);
    assert!(size_of::<ExtensibleNode>() == 16);

    // ExtensibleNodeMethods: char* + Size + four fn pointers.
    assert!(size_of::<ExtensibleNodeMethods>() == 6 * size_of::<usize>());
    assert!(offset_of!(ExtensibleNodeMethods, node_size) == size_of::<usize>());

    // The unified resolver is stored in an `AtomicPtr<()>`, so its function
    // pointer must be data-pointer sized (true on every platform this backend
    // targets, where C function and data pointers are interconvertible). Pin it
    // so a platform that violated it fails to compile rather than misbehave.
    assert!(size_of::<ExtensibleNodeMethodsResolver>() == size_of::<*mut ()>());
};

// ---------------------------------------------------------------------------
// Coverage registration.
// ---------------------------------------------------------------------------

use crate::{NodeTypeCoverage, NodeTypeStatus};

/// Node types modelled by the planner / extensible family.
///
/// Only structs that copyfuncs **or** equalfuncs handle appear here (those NOT
/// marked both `no_copy` and `no_equal`). `AppendRelInfo` - also copy/equal
/// supported - is registered by [`crate::parsenodes`], not here, to keep each
/// node in exactly one coverage slice. The opaque forward declarations
/// (`EquivalenceClass`, `EquivalenceMember`, `MergeScanSelCache`) are *not*
/// registered: they are seam pointees that copy/equal never traverse.
pub fn node_types_covered() -> &'static [NodeTypeStatus] {
    use crate::node_tags::*;
    const fn m(name: &'static str, tag: NodeTag) -> NodeTypeStatus {
        NodeTypeStatus {
            name,
            tag,
            coverage: NodeTypeCoverage::Modelled,
        }
    }
    const TABLE: &[NodeTypeStatus] = &[
        m("PathKey", T_PathKey),
        m("GroupByOrdering", T_GroupByOrdering),
        m("RestrictInfo", T_RestrictInfo),
        m("PlaceHolderVar", T_PlaceHolderVar),
        m("SpecialJoinInfo", T_SpecialJoinInfo),
        m("PlaceHolderInfo", T_PlaceHolderInfo),
        m("ForeignKeyCacheInfo", T_ForeignKeyCacheInfo),
        m("ExtensibleNode", T_ExtensibleNode),
    ];
    TABLE
}
