//! Fmgr-adjacent catalog limits (`pg_config_manual.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/fmgr.rs
//! as ports need items; only the items currently consumed are present.

pub const INDEX_MAX_KEYS: i32 = 32;
pub const NAMEDATALEN: i32 = 64;

/// `IOFuncSelector` (`fmgr.h` / `utils/lsyscache.h`) — which I/O direction a
/// type's I/O function lookup (`get_type_io_data`, `get_range_io_data`,
/// `get_multirange_io_data`) resolves a proc for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IOFuncSelector {
    /// `IOFunc_input`
    Input,
    /// `IOFunc_output`
    Output,
    /// `IOFunc_receive`
    Receive,
    /// `IOFunc_send`
    Send,
}

/// `PG_VERSION_NUM` (`pg_config.h`) — numeric server version, 180003 for
/// PostgreSQL 18.3.
pub const PG_VERSION_NUM: i32 = 180_003;

/// `FLOAT8PASSBYVAL` (`c.h`) — `true` (1) on the 64-bit build platforms.
pub const FLOAT8PASSBYVAL: i32 = 1;

/// `FMGR_ABI_EXTRA` (`pg_config_manual.h`) — `"PostgreSQL"`, NUL-padded into
/// the 32-byte `abi_extra` field of a magic block.
pub const FMGR_ABI_EXTRA: [u8; 32] = [
    b'P', b'o', b's', b't', b'g', b'r', b'e', b'S', b'Q', b'L', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

/// `PG_MAGIC_FUNCTION_NAME_STRING` (`fmgr.h`) — the symbol a module exports to
/// provide its magic block (`dlsym` target in `internal_load_library`).
pub const PG_MAGIC_FUNCTION_NAME_STRING: &str = "Pg_magic_func";

/// `_PG_init` (`fmgr.h`) — the optional per-module initializer symbol.
pub const PG_INIT_FUNCTION_NAME_STRING: &str = "_PG_init";

/// `Pg_abi_values` (`fmgr.h`) — the values checked to verify ABI
/// compatibility of a dynamically loaded module. C compares these with
/// `memcmp`, so the struct deliberately has no padding; the port compares them
/// field-for-field (`internal_load_library`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgAbiValues {
    /// PostgreSQL major version (`PG_VERSION_NUM / 100`).
    pub version: i32,
    /// `FUNC_MAX_ARGS`.
    pub funcmaxargs: i32,
    /// `INDEX_MAX_KEYS`.
    pub indexmaxkeys: i32,
    /// `NAMEDATALEN`.
    pub namedatalen: i32,
    /// `FLOAT8PASSBYVAL`.
    pub float8byval: i32,
    /// `FMGR_ABI_EXTRA` — product-identity string, NUL-padded.
    pub abi_extra: [u8; 32],
}

impl PgAbiValues {
    /// `PG_MODULE_ABI_DATA` (`fmgr.h`) — the ABI values this server build
    /// requires a module to match.
    pub const fn server() -> Self {
        Self {
            version: PG_VERSION_NUM / 100,
            funcmaxargs: crate::primitive::FUNC_MAX_ARGS as i32,
            indexmaxkeys: INDEX_MAX_KEYS,
            namedatalen: NAMEDATALEN,
            float8byval: FLOAT8PASSBYVAL,
            abi_extra: FMGR_ABI_EXTRA,
        }
    }
}

/// `FmgrInfo` (`fmgr.h`) — function-call lookup data filled in by `fmgr_info()`
/// and read by every fmgr caller.
///
/// C's struct also caches the resolved call address (`fn_addr`), the owning
/// memory context (`fn_mcxt`), handler scratch space (`fn_extra`), and the
/// parse tree (`fn_expr`). The fields modelled here are the ones the executor's
/// expression compiler reads *after* `fmgr_info()` to pick an opcode and stamp
/// a step payload:
///
/// * `fn_strict` / `fn_stats` — `ExecInitFunc` (execExpr.c:2788-2805) selects
///   the `EEOP_FUNCEXPR{,_STRICT,_FUSAGE}` variant from these; the agg trans
///   (3901), agg deserialize (3797), and hash (4084-4097) builders likewise
///   pick the strict vs non-strict opcode from `flinfo->fn_strict`.
/// * `fn_addr` — stamped onto the `func`/`hashdatum`/`scalararrayop`/
///   `rowcompare`/`minmax` step payloads as the actual call address.
/// * `fn_nargs` / `fn_retset` — argument count and set-returning flag the
///   builders read when laying down fcinfo.
///
/// `fn_addr` is held as an opaque address ([`usize`]) rather than a typed
/// function pointer: the `PGFunction` shape lives in the nodes layer
/// (`types-nodes`) and the call-site step payloads carry their own typed
/// `fn_addr`, so types-core (which must not depend on types-nodes) keeps only
/// the raw address `fmgr_info()` resolved. `0` means unresolved.
///
/// `fn_expr` is the faithful rendering of C's `fmNodePtr fn_expr` — the
/// call-expression node `fmgr_info_set_expr()` stamps onto a resolved
/// `FmgrInfo` so that `get_fn_expr_argtype`/`get_fn_expr_rettype` can read the
/// declared argument/result types (load-bearing for polymorphic, by-ref, and
/// ordered-set transition/finalize functions). The node value lives in the
/// `types-nodes` arena vocabulary (`primnodes::Expr`), which types-core must
/// not name; it is therefore carried *erased* through [`FnExprErased`]
/// (`Arc<dyn Any>`). A crate that does depend on `types-nodes` boxes the
/// owned `Expr` in and downcasts it back out — the established cross-layer
/// erased-bridge idiom, not a new opaque-handle divergence (it is the only
/// sound representation of a no-`types-nodes` struct pointing at an arena
/// `Node`). The `Rc` keeps `FmgrInfo` `Clone` (C copies the bare pointer; the
/// owned model shares the node).
///
/// Adding `fn_expr` costs `FmgrInfo` its `Copy`/`Eq`/`PartialEq` derives (an
/// `Rc<dyn Any>` is neither): callers move/clone the struct and never compare
/// two `FmgrInfo`s for equality (C compares neither).
#[derive(Clone, Debug, Default)]
pub struct FmgrInfo {
    /// `PGFunction fn_addr` — resolved call address, as an opaque pointer value
    /// (`0` = unresolved). The typed callable is re-derived at the step payload
    /// layer that owns the `PGFunction` type.
    pub fn_addr: usize,
    /// OID of the function (`pg_proc` OID).
    pub fn_oid: crate::primitive::Oid,
    /// `short fn_nargs` — number of input args (0..`FUNC_MAX_ARGS`).
    pub fn_nargs: i16,
    /// `bool fn_strict` — function is "strict" (NULL in => NULL out). Drives the
    /// strict-opcode selection in the executor's expression compiler.
    pub fn_strict: bool,
    /// `bool fn_retset` — function returns a set.
    pub fn_retset: bool,
    /// `unsigned char fn_stats` — collect stats if `track_functions > this`.
    pub fn_stats: u8,
    /// `fmNodePtr fn_expr` — the call-expression node `fmgr_info_set_expr()`
    /// stamps on, carried erased ([`FnExprErased`]). `None` is C's `NULL`
    /// (no call expression — `get_fn_expr_*` then return `InvalidOid`/`false`).
    /// Default / `empty()` leave it `None`, matching `fmgr_info()`'s zeroed
    /// frame before `fmgr_info_set_expr()`.
    pub fn_expr: Option<FnExprErased>,
}

/// Erased carrier for `FmgrInfo.fn_expr` (C's `fmNodePtr`).
///
/// The call-expression node is a `types_nodes::primnodes::Expr` — a type
/// types-core must not name (the no-`types-nodes` rule). It is held behind
/// `Rc<dyn Any>` so types-core only names `core::any::Any`: a crate that
/// depends on `types-nodes` constructs it from an owned `Expr`
/// ([`FnExprErased::new`]) and downcasts it back ([`FnExprErased::downcast_ref`])
/// to read argument/result types. `Rc` (not `Box`) so `FmgrInfo` stays `Clone`
/// — C copies the bare `fn_expr` pointer when an `FmgrInfo` is copied; the
/// owned model shares the node through the reference count, equivalently
/// non-owning from the node's perspective (the arena owns the `Expr`).
///
/// `Rc<dyn Any>` is *not* `Send`/`Sync` — and `types_nodes::primnodes::Expr`
/// is itself neither (it holds arena `PgBox`/`Rc`/`Cell`/`dyn ExpandedObject`
/// non-thread-shared payloads), so a `Send + Sync` bound would be unsatisfiable
/// anyway. This matches the single-backend execution model: an `FmgrInfo` (like
/// every `Expr`-bearing executor structure) lives on one backend's thread, never
/// crossing a thread boundary, exactly as C's per-backend `FmgrInfo` does.
#[derive(Clone)]
pub struct FnExprErased(alloc::rc::Rc<dyn core::any::Any>);

impl FnExprErased {
    /// Box an owned call-expression node into the erased carrier. `T` is the
    /// `types-nodes` expression type (`primnodes::Expr`); only a
    /// `types-nodes`-depending crate names it.
    pub fn new<T: core::any::Any>(expr: T) -> Self {
        Self(alloc::rc::Rc::new(expr))
    }

    /// Downcast the erased node back to `&T` (the concrete expression type a
    /// `types-nodes`-depending reader knows), or `None` if it is some other
    /// type. Mirrors C reading through the `fmNodePtr`.
    pub fn downcast_ref<T: core::any::Any>(&self) -> Option<&T> {
        self.0.downcast_ref::<T>()
    }
}

impl core::fmt::Debug for FnExprErased {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // The erased node has no Debug; mirror C's opaque pointer.
        f.write_str("FnExprErased(<fn_expr node>)")
    }
}

impl FmgrInfo {
    /// An unresolved `FmgrInfo` (`fn_oid = InvalidOid`, no address, not strict).
    pub fn empty() -> Self {
        Self {
            fn_addr: 0,
            fn_oid: 0,
            fn_nargs: 0,
            fn_strict: false,
            fn_retset: false,
            fn_stats: 0,
            fn_expr: None,
        }
    }
}

/// `F_INT4EQ` (`catalog/fmgroids.h`) — `int4eq`, pg_proc OID 65
/// (`pg_proc.dat`).
pub const F_INT4EQ: crate::primitive::RegProcedure = 65;
/// `F_INT4GE` (`catalog/fmgroids.h`) — `int4ge`, pg_proc OID 150
/// (`pg_proc.dat`).
pub const F_INT4GE: crate::primitive::RegProcedure = 150;
/// `F_OIDEQ` (`catalog/fmgroids.h`) — `oideq`, pg_proc OID 184
/// (`pg_proc.dat`).
pub const F_OIDEQ: crate::primitive::RegProcedure = 184;
/// `F_TEXTEQ` (`catalog/fmgroids.h`) — `texteq`, pg_proc OID 67
/// (`pg_proc.dat`).
pub const F_TEXTEQ: crate::primitive::RegProcedure = 67;
/// `F_INT2EQ` (`catalog/fmgroids.h`) — `int2eq`, pg_proc OID 63
/// (`pg_proc.dat`).
pub const F_INT2EQ: crate::primitive::RegProcedure = 63;
/// `F_INT2GT` (`catalog/fmgroids.h`) — `int2gt`, pg_proc OID 146
/// (`pg_proc.dat`).
pub const F_INT2GT: crate::primitive::RegProcedure = 146;
/// `F_NAMEEQ` (`catalog/fmgroids.h`) — `nameeq`, pg_proc OID 62
/// (`pg_proc.dat`).
pub const F_NAMEEQ: crate::primitive::RegProcedure = 62;
/// `F_BOOLEQ` (`catalog/fmgroids.h`) — `booleq`, pg_proc OID 60
/// (`pg_proc.dat`).
pub const F_BOOLEQ: crate::primitive::RegProcedure = 60;
/// `F_CHAREQ` (`catalog/fmgroids.h`) — `chareq`, pg_proc OID 61
/// (`pg_proc.dat`).
pub const F_CHAREQ: crate::primitive::RegProcedure = 61;
/// `F_CHARNE` (`catalog/fmgroids.h`) — `charne`, pg_proc OID 70
/// (`pg_proc.dat`).
pub const F_CHARNE: crate::primitive::RegProcedure = 70;

#[cfg(test)]
mod tests {
    use super::*;

    // A stand-in for a `types-nodes` `Expr` (types-core can't name the real
    // one): proves the erased `fn_expr` round-trips through `Arc<dyn Any>` and
    // downcasts back to the concrete reader type, the mechanism the
    // `fmgr_info_set_expr` / `get_fn_expr_*` seams rely on.
    #[derive(Debug, PartialEq)]
    struct FakeExpr {
        argtypes: [u32; 2],
    }

    #[test]
    fn fn_expr_erased_round_trips() {
        let mut finfo = FmgrInfo::empty();
        assert!(finfo.fn_expr.is_none());

        // fmgr_info_set_expr: stamp the call-expression node.
        let expr = FakeExpr { argtypes: [23, 25] };
        finfo.fn_expr = Some(FnExprErased::new(expr));

        // get_fn_expr_*: a types-nodes-aware reader downcasts it back.
        let recovered = finfo
            .fn_expr
            .as_ref()
            .and_then(|e| e.downcast_ref::<FakeExpr>())
            .expect("fn_expr downcasts to the stamped type");
        assert_eq!(recovered.argtypes, [23, 25]);

        // Cloning the FmgrInfo shares the node (Rc), as C copies the pointer.
        let cloned = finfo.clone();
        let recovered2 = cloned
            .fn_expr
            .as_ref()
            .and_then(|e| e.downcast_ref::<FakeExpr>())
            .expect("cloned fn_expr still downcasts");
        assert_eq!(recovered2.argtypes, [23, 25]);

        // A wrong-type downcast yields None (no panic), the C NULL fall-through.
        assert!(finfo
            .fn_expr
            .as_ref()
            .and_then(|e| e.downcast_ref::<u64>())
            .is_none());
    }
}
