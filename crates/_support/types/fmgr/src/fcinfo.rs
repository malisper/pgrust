use alloc::boxed::Box;
use alloc::format;
use core::any::{Any, TypeId};
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
    pub fn_extra: Option<FnExtra>,
    pub fn_expr: Option<FnExprErased>,
}

// C's thin `void *fn_extra` shape: one pointer in FmgrInfo; the pointee leads
// with (TypeId, dropper) so the memo hit path is a dependent load + compare
// instead of `Box<dyn Any>`'s vtable load + indirect type_id() call + two
// 128-bit inline constants (~35-45 insns/hit vs C's 4-insn pointer read).
// Lifetime contract (the repo invariant C relies on): fn_extra lives exactly
// as long as its FmgrInfo; the memo type is statically bound to the resolved
// function, so a TypeId mismatch is the wiring bug C corrupts memory on.
pub struct FnExtra {
    ptr: NonNull<FnExtraHeader>,
}

#[repr(C)]
struct FnExtraHeader {
    type_id: TypeId,
    drop_fn: unsafe fn(*mut FnExtraHeader),
}

#[repr(C)]
struct FnExtraBox<T> {
    header: FnExtraHeader,
    value: T,
}

unsafe fn drop_fn_extra_box<T>(p: *mut FnExtraHeader) {
    // SAFETY: `p` was minted by FnExtra::new::<T> from a Box<FnExtraBox<T>>
    // (header at offset 0 per repr(C)) and is dropped exactly once.
    drop(unsafe { Box::from_raw(p.cast::<FnExtraBox<T>>()) });
}

impl FnExtra {
    pub fn new<T: Any>(value: T) -> Self {
        let boxed = Box::new(FnExtraBox {
            header: FnExtraHeader {
                type_id: TypeId::of::<T>(),
                drop_fn: drop_fn_extra_box::<T>,
            },
            value,
        });
        FnExtra {
            ptr: NonNull::from(Box::leak(boxed)).cast(),
        }
    }

    // Release trusts the wiring (C's fn_extra stance: the memo type is
    // statically bound to the resolved function, so a mismatch is the same
    // wiring bug C corrupts memory on); debug builds + Miri panic loudly.
    // The always-on compare cost ~12 insns/hit (16B inline TypeId constant +
    // ldp + ccmp) on paths whose entire C budget is 4.
    #[inline]
    fn check<T: Any>(&self) {
        if cfg!(debug_assertions)
            // SAFETY: `ptr` points at a live FnExtraBox<_> whose header leads it.
            && unsafe { self.ptr.as_ref() }.type_id != TypeId::of::<T>()
        {
            fn_extra_type_mismatch::<T>();
        }
    }

    #[inline]
    pub fn downcast_ref<T: Any>(&self) -> &T {
        self.check::<T>();
        // SAFETY: the pointee is FnExtraBox<T> per the wiring contract above
        // (debug-checked via TypeId); shared borrow of self keeps it live and
        // un-mutated.
        unsafe { &self.ptr.cast::<FnExtraBox<T>>().as_ref().value }
    }

    #[inline]
    pub fn downcast_mut<T: Any>(&mut self) -> &mut T {
        self.check::<T>();
        // SAFETY: as downcast_ref, with unique access through &mut self.
        unsafe { &mut self.ptr.cast::<FnExtraBox<T>>().as_mut().value }
    }
}

impl Drop for FnExtra {
    fn drop(&mut self) {
        // SAFETY: `ptr` is the live allocation FnExtra::new minted; drop_fn is
        // its type's dropper; Drop runs once.
        unsafe {
            let drop_fn = self.ptr.as_ref().drop_fn;
            drop_fn(self.ptr.as_ptr());
        }
    }
}

#[cold]
#[inline(never)]
fn fn_extra_type_mismatch<T>() -> ! {
    panic!(
        "fmgr fn_extra: downcast to {} failed",
        core::any::type_name::<T>()
    )
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

// Thin call ABI: the callee must never read flinfo, never write
// fcinfo.isnull, and read at most its registered arity of args; error
// surface identical to its PGFunction wrapper (same core body).
pub type ThinFcinfo = LocalFcinfo<0>;
pub type PGFunctionThin = unsafe fn(NonNull<ThinFcinfo>) -> PgResult<Datum>;

/// # Safety
/// `fcinfo` is a live fcinfo image with more than `argno` args.
#[inline(always)]
pub unsafe fn thin_arg(fcinfo: NonNull<ThinFcinfo>, argno: usize) -> NullableDatum {
    const OFF: usize = core::mem::offset_of!(LocalFcinfo<1>, args);
    // SAFETY: caller contract — argno is inside the image's args tail.
    unsafe {
        fcinfo
            .cast::<u8>()
            .add(OFF + argno * core::mem::size_of::<NullableDatum>())
            .cast::<NullableDatum>()
            .read()
    }
}

// A zero the optimizer cannot see through: minted in a register by a pure,
// register-only asm (no memory operand, no clobbers), so a plain byte store
// of it is the frame's LAST write to isnull that no pass can widen into — or
// fold under — a covering constant store (the value is unknown). That keeps
// the emitted isnull init a byte store wherever the callee stays out of line
// (the immediately-following isnull byte reload only hits store-to-load
// forwarding on the wide arm cores when the youngest covering store is the
// byte itself — a merged word store covering it mid-span is a measured hard
// stall), while costing inlined call sites nothing: unlike a volatile store
// — whose ordering semantics pin the frame and defeat store merging/DSE at
// every inlined call site, a measured engine-wide loss under fat LTO — a
// plain store of an opaque value can still be dead-store-eliminated, and the
// asm is pure/nomem so it hoists, CSEs, and erects no memory barrier. The
// only residue at fully-inlined sites is that the post-call isnull check
// compares a register instead of constant-folding away.
#[inline(always)]
fn opaque_false() -> u8 {
    #[cfg(all(target_arch = "aarch64", not(miri)))]
    {
        let r: u8;
        // SAFETY: register-only asm; writes one output register, touches no
        // memory, preserves flags.
        unsafe {
            core::arch::asm!(
                "mov {r:w}, wzr",
                r = out(reg) r,
                options(pure, nomem, nostack, preserves_flags)
            );
        }
        r
    }
    #[cfg(any(not(target_arch = "aarch64"), miri))]
    {
        // No forwarding hazard measured off arm (and Miri cannot execute
        // asm); keep the init foldable.
        0u8
    }
}

// Layout vs C fmgr.h (LP64): NullableDatum 16 == 16; header 32 == 32
// (result_mcx rides where C keeps flinfo); fcinfo(2) 64 <= 64; FmgrInfo 48
// == 48 (thin fn_extra; fat fn_expr +8, fn_mcxt dropped; rule-9 cap 128).
const _: () = {
    assert!(core::mem::size_of::<NullableDatum>() == 16);
    assert!(core::mem::offset_of!(LocalFcinfo<0>, args) <= 32);
    assert!(core::mem::size_of::<LocalFcinfo<0>>() <= 32);
    assert!(core::mem::size_of::<LocalFcinfo<2>>() <= 64);
    assert!(core::mem::size_of::<FmgrInfo>() <= 48);
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

    // Fresh per-call frame with the (fncollation, isnull, pad, nargs) header
    // rewritten by rearm(): naive field inits get merged by LLVM into a store
    // covering isnull at neither its start nor middle, and the post-call
    // isnull byte reload then misses store-to-load forwarding on the wide
    // arm cores (graviton.md §4.5) — a measured 1.5× ns stall on the 2-arg
    // call loop at 0.86× instructions. See rearm() for the store discipline.
    #[inline]
    pub fn fresh(collation: Oid) -> Self {
        const {
            // wasm32: 4-byte pointers put fncollation at +12. The raw header
            // writes need only the +4/+6 field relations (asserted below;
            // the u32/u8/u16 stores are individually aligned) — the 8B-span
            // alignment is the native store-merge optimization's
            // precondition, not a correctness bound.
            #[cfg(not(target_family = "wasm"))]
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

    // Persistent-carrier reuse: reset (fncollation, isnull, nargs) with the
    // header store discipline below; args are rewritten in place by the
    // caller.
    #[inline]
    pub fn rearm(&mut self, collation: Oid) {
        const {
            // wasm32: 4-byte pointers put fncollation at +12. The raw header
            // writes need only the +4/+6 field relations (asserted below;
            // the u32/u8/u16 stores are individually aligned) — the 8B-span
            // alignment is the native store-merge optimization's
            // precondition, not a correctness bound.
            #[cfg(not(target_family = "wasm"))]
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
        // isnull's YOUNGEST covering store must be a byte store (C's `strb`),
        // or the post-call `ldrb` reload misses store-to-load forwarding on
        // the wide arm cores (measured: that reload+cmp carried 80% of the
        // call loop's cycles — 0.79x instr but 2.07x ns). Refuted shapes:
        // field-wise constant inits (LLVM merges them into a store covering
        // isnull mid-span); one 8B header word + trailing volatile byte
        // (LLVM legally sinks the word's pieces PAST the volatile, leaving a
        // misaligned `stur` covering isnull as its last byte); and a volatile
        // byte itself, which holds the shape but pins every inlined call
        // site's frame against store merging/DSE — a measured engine-wide
        // regression under fat LTO. The opaque_false() plain byte store is
        // stable WITHOUT the volatile poison: its value is unknown, so no
        // pass can widen it into, or sink a covering constant store past,
        // the byte — while dead frames still fold away.
        // SAFETY: asserted above — (fncollation, isnull, pad, nargs) is an
        // 8-aligned 8B span. Pointer is derived from `self` so its provenance
        // covers the whole span (a field-raw pointer would carry 4B
        // provenance: Miri SB violation).
        unsafe {
            let p = (self as *mut Self)
                .cast::<u8>()
                .add(core::mem::offset_of!(LocalFcinfo<N>, fncollation));
            p.cast::<u32>().write(collation);
            p.add(4).write(opaque_false());
            p.add(6).cast::<u16>().write(N as u16);
        }
    }

    // Fresh frame built IN PLACE: every field written exactly once and isnull
    // ONLY via the opaque byte store. `fresh()` (new() + rearm()) still
    // carries new()'s constant isnull=false init; with a COMPILE-TIME
    // collation (the define_calls literal-0 callers) LLVM merges that init
    // with the adjacent zero fields into one covering store — with the
    // opaque store as the byte's unremovable-by-merging LAST writer that is
    // harmless (the covering store lands before it), but a fully constant
    // frame init invites the mid-span covering shape that misses
    // store-to-load forwarding on the byte reload. With isnull written
    // exactly once, opaquely, the youngest covering store is a byte store on
    // every path. No move-out: returning Self by value would recopy the
    // frame with plain stores.
    #[inline(always)]
    pub fn init_in_place(slot: &mut core::mem::MaybeUninit<Self>, collation: Oid) -> &mut Self {
        const {
            // wasm32: 4-byte pointers put fncollation at +12. The raw header
            // writes need only the +4/+6 field relations (asserted below;
            // the u32/u8/u16 stores are individually aligned) — the 8B-span
            // alignment is the native store-merge optimization's
            // precondition, not a correctness bound.
            #[cfg(not(target_family = "wasm"))]
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
        let p = slot.as_mut_ptr();
        // SAFETY: p is valid uninitialized storage for Self; every field is
        // written exactly once below (the byte between isnull and nargs is
        // repr(C) padding and may stay uninit); the header span is 8-aligned
        // per the const asserts and p's provenance covers the whole struct.
        unsafe {
            (&raw mut (*p).context).write(None);
            (&raw mut (*p).resultinfo).write(None);
            (&raw mut (*p).result_mcx).write(None);
            let h = p
                .cast::<u8>()
                .add(core::mem::offset_of!(LocalFcinfo<N>, fncollation));
            h.cast::<u32>().write(collation);
            h.add(4).write(opaque_false());
            h.add(6).cast::<u16>().write(N as u16);
            (&raw mut (*p).args).write([NullableDatum::null(); N]);
            &mut *p
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

    /// # Safety
    /// `'a` must not exceed set_result_mcx's liveness window (until re-arm or
    /// frame death); the detached lifetime lets a callee hold the mcx (e.g.
    /// in a `MaterializedSRF`) while also passing `&mut self` around.
    #[inline]
    pub unsafe fn result_mcx_detached<'a>(&self) -> Mcx<'a> {
        match self.result_mcx {
            // SAFETY: caller upholds the window above.
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
        // zeroed isnull/pad) where C pays a str+strb pair. u64 words, NOT
        // usize: the Datum word is 8 bytes on every target (ILP32 wasm32
        // included), and a usize pair there would truncate the datum to its
        // low 32 bits and leave isnull stale. Identical codegen on 64-bit.
        unsafe {
            core::ptr::from_mut(slot)
                .cast::<[u64; 2]>()
                .write([value.as_u64(), 0]);
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
        self.fn_extra = Some(FnExtra::new(state));
    }

    pub fn has_fn_extra(&self) -> bool {
        self.fn_extra.is_some()
    }

    #[inline]
    pub fn fn_extra_ref<T: Any>(&self) -> Option<&T> {
        Some(self.fn_extra.as_ref()?.downcast_ref::<T>())
    }

    #[inline]
    pub fn fn_extra_mut<T: Any>(&mut self) -> Option<&mut T> {
        Some(self.fn_extra.as_mut()?.downcast_mut::<T>())
    }

    /// C set_fn_opclass_options: options ride fn_expr (support procs are
    /// never called inside expressions, so the slot is free).
    /// # Safety
    /// `opts` must outlive every invocation through this FmgrInfo (C's
    /// contract: the options Const lives in rd_indexcxt; here the owner
    /// holding the FmgrInfo also owns the boxed options).
    pub unsafe fn set_opclass_options(&mut self, opts: &OpclassOptions) {
        // SAFETY: caller upholds the pointee-outlives-reads contract.
        self.fn_expr = Some(unsafe { FnExprErased::from_node_ref(opts) });
    }

    /// C get_fn_opclass_options / has_fn_opclass_options; None = C NULL
    /// options (AM reads compiled-in defaults).
    #[inline]
    pub fn opclass_options(&self) -> Option<&[u8]> {
        self.fn_expr
            .as_ref()?
            .downcast_ref::<OpclassOptions>()
            .map(|o| &o.0[..])
    }
}

// The parsed opclass options struct image (varlena header + fields at C
// offsetof positions), boxed so its address is stable while carriers move.
pub struct OpclassOptions(pub Box<[u8]>);

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

#[track_caller]
#[cold]
#[inline(never)]
fn returned_null_oid(fn_oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("function {fn_oid} returned NULL")))
}

#[track_caller]
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
            let mut slot = core::mem::MaybeUninit::<LocalFcinfo<$n>>::uninit();
            let fcinfo = LocalFcinfo::<$n>::init_in_place(&mut slot, collation);
            $(fcinfo.set_arg($idx, $arg);)+
            let result = flinfo.invoke(&mut *fcinfo)?;
            if fcinfo.isnull {
                return Err(returned_null_oid(flinfo.fn_oid));
            }
            Ok(result)
        }

        #[inline]
        pub fn $dname(func: PGFunction, collation: Oid, $($arg: Datum,)+) -> PgResult<Datum> {
            let mut slot = core::mem::MaybeUninit::<LocalFcinfo<$n>>::uninit();
            let fcinfo = LocalFcinfo::<$n>::init_in_place(&mut slot, collation);
            $(fcinfo.set_arg($idx, $arg);)+
            let result = func(None, fcinfo)?;
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
            let mut slot = core::mem::MaybeUninit::<LocalFcinfo<$n>>::uninit();
            let fcinfo = LocalFcinfo::<$n>::init_in_place(&mut slot, collation);
            // SAFETY: `mcx` outlives this stack frame's single call.
            unsafe { fcinfo.set_result_mcx(mcx) };
            $(fcinfo.set_arg($idx, $arg);)+
            let result = flinfo.invoke(&mut *fcinfo)?;
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
            let mut slot = core::mem::MaybeUninit::<LocalFcinfo<$n>>::uninit();
            let fcinfo = LocalFcinfo::<$n>::init_in_place(&mut slot, collation);
            // SAFETY: `mcx` outlives this stack frame's single call.
            unsafe { fcinfo.set_result_mcx(mcx) };
            $(fcinfo.set_arg($idx, $arg);)+
            let result = func(None, fcinfo)?;
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

// `func` referees the registration: `thin` is handed out only for a carrier
// whose fn_addr is exactly `func`; any diverging resolution falls back.
#[derive(Clone, Copy)]
pub struct ThinBuiltin {
    pub foid: Oid,
    pub nargs: i16,
    pub func: PGFunction,
    pub thin: PGFunctionThin,
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
