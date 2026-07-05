pub const INDEX_MAX_KEYS: i32 = 32;
pub const NAMEDATALEN: i32 = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IOFuncSelector {
    Input,
    Output,
    Receive,
    Send,
}

pub const PG_VERSION_NUM: i32 = 180_003;

pub const FLOAT8PASSBYVAL: i32 = 1;

pub const FMGR_ABI_EXTRA: [u8; 32] = [
    b'P', b'o', b's', b't', b'g', b'r', b'e', b'S', b'Q', b'L', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

pub const PG_MAGIC_FUNCTION_NAME_STRING: &str = "Pg_magic_func";

pub const PG_INIT_FUNCTION_NAME_STRING: &str = "_PG_init";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgAbiValues {
    pub version: i32,
    pub funcmaxargs: i32,
    pub indexmaxkeys: i32,
    pub namedatalen: i32,
    pub float8byval: i32,
    pub abi_extra: [u8; 32],
}

impl PgAbiValues {
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

// `fn_addr` is an opaque address (`0` = unresolved): the typed `PGFunction`
// shape lives in the nodes layer, which this crate must not depend on.
#[derive(Clone, Debug, Default)]
pub struct FmgrInfo {
    pub fn_addr: usize,
    pub fn_oid: crate::primitive::Oid,
    pub fn_nargs: i16,
    pub fn_strict: bool,
    pub fn_retset: bool,
    pub fn_stats: u8,
    // C's `fmNodePtr fn_expr`, carried erased; `None` is C's NULL.
    pub fn_expr: Option<FnExprErased>,
}

// C: build_aggregate_transfn_expr/build_aggregate_finalfn_expr's constructed
// FuncExpr, reduced to its consumers (get_fn_expr_argtype +
// get_fn_expr_rettype/get_call_result_type): `rettype` is the fake FuncExpr's
// funcresulttype (the transtype for transfns, the aggregate result type for
// finalfns); `argtypes` slot 0 is the transition type, slots 1.. the
// aggregate input types. The slice is arena-backed with the lifetime
// forgotten (from_node_ref's contract); Copy, so FmgrInfo stays drop-free.
#[derive(Clone, Copy)]
pub struct AggFnArgTypes {
    pub rettype: crate::primitive::Oid,
    pub argtypes: &'static [crate::primitive::Oid],
}

// Erased `fn_expr` carrier: the node is a `types-nodes` `Expr` this crate
// must not name. Copy raw pointer — C copies the bare `fmNodePtr`; the arena
// owns the node, so FmgrInfo carries no drop glue.
#[derive(Clone, Copy)]
pub struct FnExprErased(*const dyn core::any::Any);

impl FnExprErased {
    /// # Safety
    /// `expr`'s backing context must outlive every `downcast_ref` read of
    /// this carrier (the resolved-once FmgrInfo dies with the plan it serves).
    /// `STATIC` must be the `'static` form of the caller's `'mcx`-branded
    /// node type (same concrete type, lifetime forgotten).
    pub unsafe fn from_node_ref<STATIC: core::any::Any>(expr: &STATIC) -> Self {
        Self(expr as &dyn core::any::Any as *const dyn core::any::Any)
    }

    pub fn downcast_ref<T: core::any::Any>(&self) -> Option<&T> {
        // SAFETY: pointee live per from_node_ref's contract.
        unsafe { (*self.0).downcast_ref::<T>() }
    }
}

impl core::fmt::Debug for FnExprErased {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("FnExprErased(<fn_expr node>)")
    }
}

impl FmgrInfo {
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

pub const F_INT4EQ: crate::primitive::RegProcedure = 65;
pub const F_INT4GE: crate::primitive::RegProcedure = 150;
pub const F_OIDEQ: crate::primitive::RegProcedure = 184;
pub const F_TEXTEQ: crate::primitive::RegProcedure = 67;
pub const F_INT2EQ: crate::primitive::RegProcedure = 63;
pub const F_INT2GT: crate::primitive::RegProcedure = 146;
pub const F_NAMEEQ: crate::primitive::RegProcedure = 62;
pub const F_BOOLEQ: crate::primitive::RegProcedure = 60;
pub const F_CHAREQ: crate::primitive::RegProcedure = 61;
pub const F_CHARNE: crate::primitive::RegProcedure = 70;

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct FakeExpr {
        argtypes: [u32; 2],
    }

    #[test]
    fn fn_expr_erased_round_trips() {
        let mut finfo = FmgrInfo::empty();
        assert!(finfo.fn_expr.is_none());

        let expr = FakeExpr { argtypes: [23, 25] };
        // SAFETY: `expr` outlives every read below.
        finfo.fn_expr = Some(unsafe { FnExprErased::from_node_ref(&expr) });

        let recovered = finfo
            .fn_expr
            .as_ref()
            .and_then(|e| e.downcast_ref::<FakeExpr>())
            .expect("fn_expr downcasts to the stamped type");
        assert_eq!(recovered.argtypes, [23, 25]);

        let cloned = finfo.clone();
        let recovered2 = cloned
            .fn_expr
            .as_ref()
            .and_then(|e| e.downcast_ref::<FakeExpr>())
            .expect("cloned fn_expr still downcasts");
        assert_eq!(recovered2.argtypes, [23, 25]);

        assert!(finfo
            .fn_expr
            .as_ref()
            .and_then(|e| e.downcast_ref::<u64>())
            .is_none());
    }
}
