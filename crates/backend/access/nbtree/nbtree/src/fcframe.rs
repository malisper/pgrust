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
        let fcinfo = &mut self.fcinfo;
        with_proc_scratch(|mcx| {
            fcinfo.rearm(key.sk_collation);
            // SAFETY: the scratch borrow spans this whole call; reset happens
            // only on the next outermost acquisition.
            unsafe { fcinfo.set_result_mcx(mcx) };
            fcinfo.set_arg(0, left);
            fcinfo.set_arg(1, right);
            let r = key.sk_func.invoke(fcinfo)?;
            if fcinfo.isnull {
                return Err(returned_null(key));
            }
            Ok(r)
        })
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
            // Inline images only: external/compressed keys take the generic
            // proc call, which detoasts (C's bttextcmp PG_GETARG_TEXT_PP).
            360 if (key.sk_collation == C_COLLATION_OID
                || key.sk_collation == POSIX_COLLATION_OID)
                && unsafe { inline_image(left) && inline_image(right) } =>
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
            // numeric_cmp: the generic proc frame is mcx-less and short args
            // detoast-expand; the kernel realigns on the stack instead.
            1769 if unsafe { inline_image(left) && inline_image(right) } => {
                // SAFETY: numeric BTORDER args are live non-null (possibly
                // packed) varlenas for the duration of the compare.
                let (a, b) = unsafe {
                    (
                        PackedVarlena::from_ptr(left.as_usize() as *const u8),
                        PackedVarlena::from_ptr(right.as_usize() as *const u8),
                    )
                };
                Ok(::adt_numeric::sortsupport::numeric_fast_cmp(a.data(), b.data()))
            }
            _ => Ok(self.call(key, left, right)?.as_i32()),
        }
    }

    /// FunctionCall2Coll(&key->sk_func, …) returning bool (operator proc).
    #[inline]
    pub fn test(&mut self, key: &mut ScanKeyData, left: Datum, right: Datum) -> PgResult<bool> {
        Ok(self.call(key, left, right)?.as_bool())
    }

    /// FunctionCall2Coll(orderproc, …) returning int32 — the so->orderProcs
    /// form used by array binary searches; same known-set dispatch as `cmp`.
    #[inline]
    pub fn cmp_proc(
        &mut self,
        proc: &mut ::types_fmgr::FmgrInfo,
        collation: ::types_core::Oid,
        left: Datum,
        right: Datum,
    ) -> PgResult<i32> {
        match proc.fn_oid {
            351 => Ok(::nbt_compare::btint4cmp(left.as_i32(), right.as_i32())),
            842 => Ok(::nbt_compare::btint8cmp(left.as_i64(), right.as_i64())),
            356 => Ok(::nbt_compare::btoidcmp(left.as_oid(), right.as_oid())),
            360 if (collation == C_COLLATION_OID || collation == POSIX_COLLATION_OID)
                && unsafe { inline_image(left) && inline_image(right) } =>
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
            1769 if unsafe { inline_image(left) && inline_image(right) } => {
                // SAFETY: as `cmp`'s numeric arm.
                let (a, b) = unsafe {
                    (
                        PackedVarlena::from_ptr(left.as_usize() as *const u8),
                        PackedVarlena::from_ptr(right.as_usize() as *const u8),
                    )
                };
                Ok(::adt_numeric::sortsupport::numeric_fast_cmp(a.data(), b.data()))
            }
            _ => {
                let fcinfo = &mut self.fcinfo;
                with_proc_scratch(|mcx| {
                    fcinfo.rearm(collation);
                    // SAFETY: as `call` — the scratch borrow spans the invoke.
                    unsafe { fcinfo.set_result_mcx(mcx) };
                    fcinfo.set_arg(0, left);
                    fcinfo.set_arg(1, right);
                    let r = proc.invoke(fcinfo)?;
                    if fcinfo.isnull {
                        return Err(proc_returned_null(proc.fn_oid));
                    }
                    Ok(r.as_i32())
                })
            }
        }
    }
}

thread_local! {
    static PROC_SCRATCH: core::cell::RefCell<::mcx::MemoryContext> =
        core::cell::RefCell::new(::mcx::MemoryContext::new_bump("btproc scratch"));
}

// Detoast/deserialize scratch for the generic proc arm (C detoasts into the
// caller's current context; the compare's by-value result never escapes).
// Reset per outermost acquisition; the borrow spans the invoke, so re-entry
// (a cmp proc whose catalog lookup runs a nested index scan) sees the cell
// busy and takes a fresh short-lived context instead of resetting the
// outer's live copies.
fn with_proc_scratch<R>(f: impl for<'s> FnOnce(::mcx::Mcx<'s>) -> PgResult<R>) -> PgResult<R> {
    PROC_SCRATCH.with(|cell| match cell.try_borrow_mut() {
        Ok(mut ctx) => {
            ctx.reset();
            f(ctx.mcx())
        }
        Err(_) => {
            let ctx = ::mcx::MemoryContext::new_bump("btproc scratch (reentrant)");
            f(ctx.mcx())
        }
    })
}

#[track_caller]
#[cold]
#[inline(never)]
fn proc_returned_null(fn_oid: ::types_core::Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("function {fn_oid} returned NULL")))
}

/// # Safety: `d` is a non-null varlena datum with a readable header byte.
#[inline]
unsafe fn inline_image(d: Datum) -> bool {
    let p = d.as_usize() as *const u8;
    // SAFETY: forwarded caller contract.
    unsafe {
        ::types_tuple::varatt::varatt_is_1b(p) && !::types_tuple::varatt::varatt_is_1b_e(p)
            || ::types_tuple::varatt::varatt_is_4b_u(p)
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn returned_null(key: &ScanKeyData) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "function {} returned NULL",
        key.sk_func.fn_oid
    )))
}
