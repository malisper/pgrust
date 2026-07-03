use ::datum::Datum;
use ::types_core::{C_COLLATION_OID, POSIX_COLLATION_OID};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::{LocalFcinfo, PackedVarlena};
use ::types_scan::scankey::ScanKeyData;

// Scan-owned fcinfo carrier: per-tuple sk_func calls rewrite collation + args
// in place — never function_call2_coll's fresh 56B frame per tuple.
pub struct OrderProcFrame {
    fcinfo: LocalFcinfo<2>,
}

impl OrderProcFrame {
    #[inline]
    pub fn new() -> Self {
        OrderProcFrame {
            fcinfo: LocalFcinfo::<2>::new(0),
        }
    }

    #[inline]
    fn call(&mut self, key: &mut ScanKeyData, left: Datum, right: Datum) -> PgResult<Datum> {
        self.fcinfo.rearm(key.sk_collation);
        self.fcinfo.set_arg(0, left);
        self.fcinfo.set_arg(1, right);
        let r = key.sk_func.invoke(&mut self.fcinfo)?;
        if self.fcinfo.isnull {
            return Err(returned_null(key));
        }
        Ok(r)
    }

    /// FunctionCall2Coll(&key->sk_func, …) returning int32 (BTORDER_PROC).
    /// Known-set procs dispatch as inlined kernels (rule 4; execexpr CmpOp
    /// precedent; fleet: 1.06x->0.96x instr); args are non-null here (strict;
    /// null keys peel off before the proc call).
    #[inline]
    pub fn cmp(&mut self, key: &mut ScanKeyData, left: Datum, right: Datum) -> PgResult<i32> {
        match key.sk_func.fn_oid {
            351 => Ok(::nbt_compare::btint4cmp(left.as_i32(), right.as_i32())),
            842 => Ok(::nbt_compare::btint8cmp(left.as_i64(), right.as_i64())),
            356 => Ok(::nbt_compare::btoidcmp(left.as_oid(), right.as_oid())),
            360 if key.sk_collation == C_COLLATION_OID
                || key.sk_collation == POSIX_COLLATION_OID =>
            {
                // SAFETY: text BTORDER args are live non-null (possibly
                // packed) varlenas for the duration of the compare.
                let (a, b) = unsafe {
                    (
                        PackedVarlena::from_ptr(left.as_usize() as *const u8),
                        PackedVarlena::from_ptr(right.as_usize() as *const u8),
                    )
                };
                Ok(::varlena::varstrfastcmp_c(a.data(), b.data()))
            }
            _ => Ok(self.call(key, left, right)?.as_i32()),
        }
    }

    /// FunctionCall2Coll(&key->sk_func, …) returning bool (operator proc).
    #[inline]
    pub fn test(&mut self, key: &mut ScanKeyData, left: Datum, right: Datum) -> PgResult<bool> {
        Ok(self.call(key, left, right)?.as_bool())
    }
}

#[cold]
#[inline(never)]
fn returned_null(key: &ScanKeyData) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "function {} returned NULL",
        key.sk_func.fn_oid
    )))
}
