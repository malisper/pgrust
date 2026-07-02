use ::datum::Datum;
use ::types_error::{PgError, PgResult};
use ::types_fmgr::LocalFcinfo;
use ::types_scan::scankey::ScanKeyData;

// The scan-owned fcinfo carrier (fmgr_core M2 watch item): one frame lives for
// a whole descent/readpage and every sk_func call rewrites collation + args in
// place — never function_call2_coll's fresh 56B frame per tuple.
pub(crate) struct OrderProcFrame {
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
    #[inline]
    pub fn cmp(&mut self, key: &mut ScanKeyData, left: Datum, right: Datum) -> PgResult<i32> {
        Ok(self.call(key, left, right)?.as_i32())
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
