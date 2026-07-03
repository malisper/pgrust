use alloc::boxed::Box;
use alloc::format;
use core::any::Any;
use core::ptr::NonNull;

use ::datum::{Datum, NullableDatum};
use ::mcx::{Mcx, MemoryContext};
use ::types_core::fmgr::FnExprErased;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};

pub const TRACK_FUNC_OFF: u8 = 0;
pub const TRACK_FUNC_PL: u8 = 1;
pub const TRACK_FUNC_ALL: u8 = 2;

// C's `fmNodePtr`: node types riding here lead with an FmNode tag (`IsA` demux).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FmNode {
    pub tag: u32,
}

pub type FmNodePtr = Option<NonNull<FmNode>>;

// C's PGFunction; `flinfo` travels as a parameter (`None` = C's NULL flinfo);
// errors return as `Err`, not an ereport longjmp.
pub type PGFunction =
    fn(Option<&mut FmgrInfo>, &mut FunctionCallInfoBaseData) -> PgResult<Datum>;

pub struct FmgrInfo {
    pub fn_addr: PGFunction,
    pub fn_oid: Oid,
    pub fn_nargs: i16,
    pub fn_strict: bool,
    pub fn_retset: bool,
    pub fn_stats: u8,
    // C's `void *fn_extra`; std Box justified: open-set slot written once per
    // resolved FmgrInfo (its lifetime replaces fn_mcxt), never per row.
    pub fn_extra: Option<Box<dyn Any>>,
    pub fn_expr: Option<FnExprErased>,
}

#[repr(C)]
pub struct FunctionCallInfoBaseData<A: ?Sized = [NullableDatum]> {
    pub context: FmNodePtr,
    pub resultinfo: FmNodePtr,
    // Result-mcx convention (C's CurrentMemoryContext for fmgr results): the
    // caller arms it pre-invoke; by-ref-returning wrappers allocate there.
    result_mcx: Option<NonNull<MemoryContext>>,
    pub fncollation: Oid,
    pub isnull: bool,
    pub nargs: i16,
    pub args: A,
}

pub type LocalFcinfo<const N: usize> = FunctionCallInfoBaseData<[NullableDatum; N]>;

// Layout vs C fmgr.h (LP64): NullableDatum 16 == 16; header 32 == 32
// (result_mcx rides where C keeps flinfo); fcinfo(2) 64 <= 64; FmgrInfo 56
// vs 48 (fat erased slots +8 each, fn_mcxt dropped; rule-9 cap 128).
const _: () = {
    assert!(core::mem::size_of::<NullableDatum>() == 16);
    assert!(core::mem::offset_of!(LocalFcinfo<0>, args) <= 32);
    assert!(core::mem::size_of::<LocalFcinfo<0>>() <= 32);
    assert!(core::mem::size_of::<LocalFcinfo<2>>() <= 64);
    assert!(core::mem::size_of::<FmgrInfo>() <= 56);
};

impl<const N: usize> LocalFcinfo<N> {
    #[inline]
    pub const fn new(collation: Oid) -> Self {
        Self {
            context: None,
            resultinfo: None,
            result_mcx: None,
            fncollation: collation,
            isnull: false,
            nargs: N as i16,
            args: [NullableDatum::null(); N],
        }
    }

    // Fresh per-call frame with (fncollation, isnull, pad, nargs) written as
    // ONE aligned 8B word: LLVM's merge of the field inits produced a store
    // covering isnull at neither its start nor middle, and the post-call
    // isnull reload then missed V2 store-to-load forwarding (graviton.md
    // §4.5) — a measured 1.5× ns stall on fmgr_call2 at 0.86× instructions.
    #[inline]
    pub fn fresh(collation: Oid) -> Self {
        const {
            assert!(core::mem::offset_of!(LocalFcinfo<N>, fncollation) % 8 == 0);
            assert!(
                core::mem::offset_of!(LocalFcinfo<N>, isnull)
                    == core::mem::offset_of!(LocalFcinfo<N>, fncollation) + 4
            );
            assert!(
                core::mem::offset_of!(LocalFcinfo<N>, nargs)
                    == core::mem::offset_of!(LocalFcinfo<N>, fncollation) + 6
            );
        }
        let mut fcinfo = Self::new(collation);
        fcinfo.rearm(collation);
        fcinfo
    }

    // Persistent-carrier reuse (the fmgr_call2 M2 watch item's remedy): reset
    // (fncollation, isnull, nargs) as the same ONE aligned 8B store; args are
    // rewritten in place by the caller.
    #[inline]
    pub fn rearm(&mut self, collation: Oid) {
        const {
            assert!(core::mem::offset_of!(LocalFcinfo<N>, fncollation) % 8 == 0);
            assert!(
                core::mem::offset_of!(LocalFcinfo<N>, isnull)
                    == core::mem::offset_of!(LocalFcinfo<N>, fncollation) + 4
            );
            assert!(
                core::mem::offset_of!(LocalFcinfo<N>, nargs)
                    == core::mem::offset_of!(LocalFcinfo<N>, fncollation) + 6
            );
        }
        let word = (collation as u64) | ((N as u16 as u64) << 48);
        // SAFETY: asserted above — (fncollation, isnull, pad, nargs) is an
        // 8-aligned 8B span; the pad byte's value is unobserved. Pointer is
        // derived from `self` so its provenance covers the whole 8B span
        // (a field-raw pointer would carry 4B provenance: Miri SB violation).
        unsafe {
            (self as *mut Self)
                .cast::<u8>()
                .add(core::mem::offset_of!(LocalFcinfo<N>, fncollation))
                .cast::<u64>()
                .write(word);
        }
    }
}

impl<const N: usize> core::ops::Deref for LocalFcinfo<N> {
    type Target = FunctionCallInfoBaseData;
    #[inline]
    fn deref(&self) -> &FunctionCallInfoBaseData {
        self
    }
}

impl<const N: usize> core::ops::DerefMut for LocalFcinfo<N> {
    #[inline]
    fn deref_mut(&mut self) -> &mut FunctionCallInfoBaseData {
        self
    }
}

impl FunctionCallInfoBaseData {
    #[inline]
    pub fn init(&mut self, nargs: i16, collation: Oid, context: FmNodePtr, resultinfo: FmNodePtr) {
        self.context = context;
        self.resultinfo = resultinfo;
        self.result_mcx = None;
        self.fncollation = collation;
        self.isnull = false;
        self.nargs = nargs;
    }

    /// # Safety
    /// The armed context must stay live (and un-reset) across every later
    /// call through this frame until it is re-armed or the frame dies.
    #[inline]
    pub unsafe fn set_result_mcx(&mut self, mcx: Mcx<'_>) {
        self.result_mcx = Some(NonNull::from(mcx.context()));
    }

    /// The armed result-ownership context; panics if never armed.
    #[inline]
    pub fn result_mcx(&self) -> Mcx<'_> {
        match self.result_mcx {
            // SAFETY: set_result_mcx's contract — the context outlives the call.
            Some(p) => unsafe { p.as_ref() }.mcx(),
            None => result_mcx_unarmed(),
        }
    }

    #[inline]
    pub fn nargs(&self) -> usize {
        if self.nargs < 0 {
            0
        } else {
            self.nargs as usize
        }
    }

    #[inline]
    pub fn get_collation(&self) -> Oid {
        self.fncollation
    }

    #[inline]
    pub fn arg(&self, index: usize) -> Datum {
        self.args[index].value
    }

    // One arity check per call instead of one bounds check per PG_GETARG.
    #[inline]
    pub fn args_n<const N: usize>(&self) -> &[NullableDatum; N] {
        if self.args.len() < N {
            arity_panic(N, self.args.len());
        }
        // SAFETY: length just checked; args are contiguous.
        unsafe { &*self.args.as_ptr().cast::<[NullableDatum; N]>() }
    }

    #[inline]
    pub fn argisnull(&self, index: usize) -> bool {
        self.args[index].isnull
    }

    #[inline]
    pub fn set_arg(&mut self, index: usize, value: Datum) {
        let slot = &mut self.args[index];
        // SAFETY: NullableDatum is a 16B/8-align POD; one 16B store (value +
        // zeroed isnull/pad) where C pays a str+strb pair.
        unsafe {
            core::ptr::from_mut(slot)
                .cast::<[usize; 2]>()
                .write([value.as_usize(), 0]);
        }
    }

    #[inline]
    pub fn set_arg_null(&mut self, index: usize) {
        self.args[index] = NullableDatum::null();
    }

    #[inline]
    pub fn has_null_args(&self) -> bool {
        let n = self.nargs();
        self.args[..n].iter().any(|a| a.isnull)
    }

    #[inline]
    pub fn return_null(&mut self) -> Datum {
        self.isnull = true;
        Datum::null()
    }
}

#[cold]
#[inline(never)]
fn result_mcx_unarmed() -> ! {
    panic!("fmgr: by-ref result needs a result mcx but the caller never armed the frame");
}

#[cold]
#[inline(never)]
fn arity_panic(wanted: usize, got: usize) -> ! {
    panic!("fmgr: callee expects {wanted} args, frame carries {got}");
}

fn unresolved_function(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    panic!("fmgr: invoked an FmgrInfo whose fn_addr was never resolved");
}

impl FmgrInfo {
    pub fn new(fn_addr: PGFunction, fn_oid: Oid, fn_nargs: i16, fn_strict: bool, fn_retset: bool) -> Self {
        Self {
            fn_addr,
            fn_oid,
            fn_nargs,
            fn_strict,
            fn_retset,
            fn_stats: TRACK_FUNC_OFF,
            fn_extra: None,
            fn_expr: None,
        }
    }

    pub fn unresolved() -> Self {
        Self::new(unresolved_function, ::types_core::primitive::InvalidOid, 0, false, false)
    }

    #[inline(always)]
    pub fn invoke(&mut self, fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
        let f = self.fn_addr;
        f(Some(self), fcinfo)
    }

    pub fn set_fn_extra<T: Any>(&mut self, state: T) {
        self.fn_extra = Some(Box::new(state));
    }

    pub fn has_fn_extra(&self) -> bool {
        self.fn_extra.is_some()
    }

    // Downcast mismatch = the wiring bug C corrupts memory on: panic loudly.
    pub fn fn_extra_ref<T: Any>(&self) -> Option<&T> {
        let any = self.fn_extra.as_ref()?;
        match any.downcast_ref::<T>() {
            Some(t) => Some(t),
            None => panic!(
                "fmgr fn_extra: downcast_ref to {} failed",
                core::any::type_name::<T>()
            ),
        }
    }

    pub fn fn_extra_mut<T: Any>(&mut self) -> Option<&mut T> {
        let any = self.fn_extra.as_mut()?;
        match any.downcast_mut::<T>() {
            Some(t) => Some(t),
            None => panic!(
                "fmgr fn_extra: downcast_mut to {} failed",
                core::any::type_name::<T>()
            ),
        }
    }
}

impl core::fmt::Debug for FmgrInfo {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FmgrInfo")
            .field("fn_oid", &self.fn_oid)
            .field("fn_nargs", &self.fn_nargs)
            .field("fn_strict", &self.fn_strict)
            .field("fn_retset", &self.fn_retset)
            .finish_non_exhaustive()
    }
}

impl Clone for FmgrInfo {
    // C fmgr_info_copy: struct copy with fn_extra reset to NULL.
    fn clone(&self) -> Self {
        Self {
            fn_addr: self.fn_addr,
            fn_oid: self.fn_oid,
            fn_nargs: self.fn_nargs,
            fn_strict: self.fn_strict,
            fn_retset: self.fn_retset,
            fn_stats: self.fn_stats,
            fn_extra: None,
            fn_expr: self.fn_expr.clone(),
        }
    }
}

#[cold]
#[inline(never)]
fn returned_null_oid(fn_oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("function {fn_oid} returned NULL")))
}

#[cold]
#[inline(never)]
fn returned_null_direct(func: PGFunction) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "function {:#x} returned NULL",
        func as usize
    )))
}

pub fn function_call0_coll(flinfo: &mut FmgrInfo, collation: Oid) -> PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<0>::new(collation);
    let result = flinfo.invoke(&mut fcinfo)?;
    if fcinfo.isnull {
        return Err(returned_null_oid(flinfo.fn_oid));
    }
    Ok(result)
}

macro_rules! define_calls {
    ($($fname:ident $dname:ident $fmname:ident $dmname:ident $n:literal ($($arg:ident $idx:tt),+);)*) => {$(
        #[inline]
        pub fn $fname(
            flinfo: &mut FmgrInfo,
            collation: Oid,
            $($arg: Datum,)+
        ) -> PgResult<Datum> {
            let mut fcinfo = LocalFcinfo::<$n>::fresh(collation);
            $(fcinfo.set_arg($idx, $arg);)+
            let result = flinfo.invoke(&mut fcinfo)?;
            if fcinfo.isnull {
                return Err(returned_null_oid(flinfo.fn_oid));
            }
            Ok(result)
        }

        #[inline]
        pub fn $dname(func: PGFunction, collation: Oid, $($arg: Datum,)+) -> PgResult<Datum> {
            let mut fcinfo = LocalFcinfo::<$n>::fresh(collation);
            $(fcinfo.set_arg($idx, $arg);)+
            let result = func(None, &mut fcinfo)?;
            if fcinfo.isnull {
                return Err(returned_null_direct(func));
            }
            Ok(result)
        }

        #[inline]
        pub fn $fmname(
            flinfo: &mut FmgrInfo,
            collation: Oid,
            mcx: Mcx<'_>,
            $($arg: Datum,)+
        ) -> PgResult<Datum> {
            let mut fcinfo = LocalFcinfo::<$n>::fresh(collation);
            // SAFETY: `mcx` outlives this stack frame's single call.
            unsafe { fcinfo.set_result_mcx(mcx) };
            $(fcinfo.set_arg($idx, $arg);)+
            let result = flinfo.invoke(&mut fcinfo)?;
            if fcinfo.isnull {
                return Err(returned_null_oid(flinfo.fn_oid));
            }
            Ok(result)
        }

        #[inline]
        pub fn $dmname(
            func: PGFunction,
            collation: Oid,
            mcx: Mcx<'_>,
            $($arg: Datum,)+
        ) -> PgResult<Datum> {
            let mut fcinfo = LocalFcinfo::<$n>::fresh(collation);
            // SAFETY: `mcx` outlives this stack frame's single call.
            unsafe { fcinfo.set_result_mcx(mcx) };
            $(fcinfo.set_arg($idx, $arg);)+
            let result = func(None, &mut fcinfo)?;
            if fcinfo.isnull {
                return Err(returned_null_direct(func));
            }
            Ok(result)
        }
    )*};
}

define_calls! {
    function_call1_coll direct_function_call1_coll function_call1_coll_in direct_function_call1_coll_in 1 (arg1 0);
    function_call2_coll direct_function_call2_coll function_call2_coll_in direct_function_call2_coll_in 2 (arg1 0, arg2 1);
    function_call3_coll direct_function_call3_coll function_call3_coll_in direct_function_call3_coll_in 3 (arg1 0, arg2 1, arg3 2);
    function_call4_coll direct_function_call4_coll function_call4_coll_in direct_function_call4_coll_in 4 (arg1 0, arg2 1, arg3 2, arg4 3);
    function_call5_coll direct_function_call5_coll function_call5_coll_in direct_function_call5_coll_in 5 (arg1 0, arg2 1, arg3 2, arg4 3, arg5 4);
    function_call6_coll direct_function_call6_coll function_call6_coll_in direct_function_call6_coll_in 6 (arg1 0, arg2 1, arg3 2, arg4 3, arg5 4, arg6 5);
    function_call7_coll direct_function_call7_coll function_call7_coll_in direct_function_call7_coll_in 7 (arg1 0, arg2 1, arg3 2, arg4 3, arg5 4, arg6 5, arg7 6);
    function_call8_coll direct_function_call8_coll function_call8_coll_in direct_function_call8_coll_in 8 (arg1 0, arg2 1, arg3 2, arg4 3, arg5 4, arg6 5, arg7 6, arg8 7);
    function_call9_coll direct_function_call9_coll function_call9_coll_in direct_function_call9_coll_in 9 (arg1 0, arg2 1, arg3 2, arg4 3, arg5 4, arg6 5, arg7 6, arg8 7, arg9 8);
}

#[derive(Clone, Copy, Debug)]
pub struct Pg_finfo_record {
    pub api_version: i32,
}

#[derive(Clone, Copy)]
pub struct FmgrBuiltin {
    pub foid: Oid,
    pub name: &'static str,
    pub nargs: i16,
    pub strict: bool,
    pub retset: bool,
    pub func: PGFunction,
}
