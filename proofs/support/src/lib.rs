//! proof_support — shared stub/support library for the Kani C≡Rust proof
//! suite (proofs/*).
//!
//! Harness crates add `proof_support = { path = "../support" }` and stop
//! hand-rolling the stubs below. Everything here is HARNESS SCAFFOLDING:
//! nothing in this crate may ever do the shipped code's job. The rules
//! (same as the `prove-target` skill):
//!
//!   * Stubs replace PLUMBING Kani cannot execute (Location::caller, format
//!     machinery, utf8-lossy loops), never logic under proof. Each stub's
//!     doc comment states its soundness contract — what leaves the proof
//!     when it is applied. Copy that wording into the family crate's module
//!     doc / ledger notes.
//!   * Error stubs are FIELD-IDENTICAL to the shipped constructors modulo
//!     the documented omissions; sqlstate stays at the shipped default so
//!     the shipped `.with_sqlstate(..)` calls remain load-bearing and can be
//!     asserted on the Err arm.
//!   * The fcinfo helpers build REAL `LocalFcinfo` frames and call the
//!     SHIPPED fc_* wrappers — they add nothing to the trusted base.
//!
//! The crate compiles under plain `cargo check` (no kani dep); the exported
//! macros expand to `#[kani::proof]` items and must be invoked inside a
//! family crate's `#[cfg(kani)]` module.

pub use fcinfo::{call, call1, call1_ok, call2, call2_ok, call_ok, fci, FcFn, ProofArg};
pub use stubs::{
    stub_env_var, stub_env_var_zero, stub_format, stub_from_utf8_lossy, stub_from_utf8_unchecked,
    stub_from_utf8_unreachable, stub_from_str_radix_unreachable, stub_once_lock_get_or_init,
    stub_pg_error_error, stub_pg_error_new,
};

pub mod stubs {
    //! Canonical `-Z stubbing` replacements. Apply with e.g.
    //! `#[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]`
    //! (runs then need `-Z stubbing`).

    use types_error::{ErrorLevel, PgError, ERROR};

    /// Stub for `types_error::PgError::error`.
    ///
    /// The shipped constructor is `#[track_caller]` and reads
    /// `core::panic::Location::caller()` (a Kani unsupported construct,
    /// kani#374) to fill the F/L wire fields. This stub builds the same
    /// struct, field-identical to the shipped `new_impl(ERROR, ..)` result
    /// except:
    ///   * `message` — text left OUT of the proof (`String::new()`; the
    ///     format machinery feeding it is stubbed separately anyway);
    ///   * `location` — shipped code fills `Some(..)`, the stub leaves
    ///     `None`; the field must not be asserted on.
    ///
    /// `sqlstate` starts at the same `default_sqlstate_for_level(ERROR)`
    /// value the shipped constructor uses, so any shipped
    /// `.with_sqlstate(..)` downstream stays load-bearing — sqlstate/level
    /// parity CAN be asserted on the Err arm (cash_pl precedent).
    ///
    /// Ledger wording when applied: "value-space + verdict (+ sqlstate)
    /// only; message text/location out of proof".
    ///
    /// Signature quirk: Kani's stub matcher requires the APIT spelling
    /// `impl Into<String>` exactly — named generics are rejected.
    pub fn stub_pg_error_error(_message: impl Into<String>) -> PgError {
        stub_pg_error_new(ERROR, _message)
    }

    /// Stub for `types_error::PgError::new` — same contract as
    /// [`stub_pg_error_error`], with the level passed through (and the
    /// sqlstate at that level's shipped default). See its doc comment.
    pub fn stub_pg_error_new(level: ErrorLevel, _message: impl Into<String>) -> PgError {
        PgError {
            level,
            sqlstate: types_error::default_sqlstate_for_level(level),
            message: String::new(),
            message_raw: None,
            detail: None,
            detail_log: None,
            hint: None,
            context: None,
            backtrace: None,
            message_id: None,
            domain: None,
            context_domain: None,
            hide_statement: false,
            hide_context: false,
            location: None,
            saved_errno: None,
            cursor_position: None,
            internal_position: None,
            internal_query: None,
            schema_name: None,
            table_name: None,
            column_name: None,
            datatype_name: None,
            constraint_name: None,
            plpgsql_context_attached: false,
        }
    }

    /// Stub for `std::fmt::format` / `alloc::fmt::format` (the engine behind
    /// `format!`): message-TEXT construction walls symex. Sound whenever the
    /// produced String feeds only error-message fields that are not asserted
    /// on — message text leaves the proof, the error VALUE/verdict stays in.
    pub fn stub_format(_args: core::fmt::Arguments<'_>) -> String {
        String::new()
    }

    /// Stub for `std::string::String::from_utf8_lossy`: its data-dependent
    /// validation/replacement loops wall symex. Same soundness contract as
    /// [`stub_format`] — only apply where the result feeds unasserted
    /// message text.
    pub fn stub_from_utf8_lossy(_v: &[u8]) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed("")
    }

    /// Stub for `core::str::from_utf8` that skips validation. Sound ONLY
    /// when every call site under proof receives bytes already proven ASCII
    /// (e.g. by a preceding `is_ascii_hexdigit` take_while) — document the
    /// argument per call site (pg_lsn precedent).
    pub fn stub_from_utf8_unchecked(v: &[u8]) -> Result<&str, core::str::Utf8Error> {
        // SAFETY (of the claim, not the code): call sites are all-ASCII by
        // harness construction; ASCII is valid UTF-8.
        Ok(unsafe { core::str::from_utf8_unchecked(v) })
    }

    /// Reachability-canary stub for `core::str::from_utf8`: panics, so a
    /// proof over a domain where the call must be UNREACHABLE (e.g. a
    /// reject-only partition) fails loudly instead of passing vacuously.
    pub fn stub_from_utf8_unreachable(_v: &[u8]) -> Result<&str, core::str::Utf8Error> {
        panic!("from_utf8 reached — harness partition predicate wrong");
    }

    /// Reachability-canary stub for `u32::from_str_radix` (see
    /// [`stub_from_utf8_unreachable`]).
    pub fn stub_from_str_radix_unreachable(
        _s: &str,
        _radix: u32,
    ) -> Result<u32, core::num::ParseIntError> {
        panic!("from_str_radix reached — harness partition predicate wrong");
    }

    /// Stub for `std::env::var`: always `Err(NotPresent)`, i.e. every
    /// env-tunable takes its documented DEFAULT arm. std's env machinery
    /// (OsStr conversion, `Chars` iteration in trim paths) drags
    /// Kani-unsupported constructs into symex. Sound when configuration is
    /// not part of the equivalence claim — but note in the harness that the
    /// proof covers the default-config arm only.
    ///
    /// Signature quirk (mirror of the APIT one above): the stub matcher
    /// requires the NAMED generic `<K: AsRef<OsStr>>` here because that is
    /// how std spells it — match the target's exact generic style.
    pub fn stub_env_var<K: AsRef<std::ffi::OsStr>>(_key: K) -> Result<String, std::env::VarError> {
        Err(std::env::VarError::NotPresent)
    }

    /// Like [`stub_env_var`] but answers `Ok("0")` — forces 0-disableable
    /// env tunables onto their DISABLED arm. Needed where the default arm
    /// itself is Kani-blocked: mcx's `local_pool_on()` default (pool
    /// stripe ON) touches `std::thread_local!` destructors, whose macOS
    /// registration (`_tlv_atexit`) is a bodyless foreign function Kani
    /// flags as reachable. `"0"` routes pool traffic to the global
    /// `PoolMutex` arm instead (plain atomics, fully modeled). Only sound
    /// when, as here, the arm choice is invisible to the equivalence claim.
    pub fn stub_env_var_zero<K: AsRef<std::ffi::OsStr>>(
        _key: K,
    ) -> Result<String, std::env::VarError> {
        Ok(String::from("0"))
    }

    /// Stub for `std::sync::OnceLock::get_or_init`: recompute instead of
    /// memoize. std's queue-based `Once` reaches thread-parking internals
    /// (`thread::current` tagged pointers → `ptr_mask`, a Kani unsupported
    /// construct). Sound when the initializer is deterministic and
    /// side-effect-free (the memoization is then unobservable); under
    /// Kani's single-thread model there is no once-racing to model.
    pub fn stub_once_lock_get_or_init<T, F>(_lock: &std::sync::OnceLock<T>, f: F) -> &T
    where
        F: FnOnce() -> T,
    {
        Box::leak(Box::new(f()))
    }
}

pub mod fcinfo {
    //! Wrapper-level call helpers (datetime-cmp/cash/scalar-misc precedent):
    //! build a real `LocalFcinfo<N>` frame and invoke a SHIPPED fc_*
    //! wrapper, so datum unwrap → core → Datum pack is inside the theorem.
    //! Scalar-datum frames cost ~nothing in the solver (measured 0.06-0.1s).

    use datum::{Datum, NullableDatum};
    use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo};

    /// The shipped fc_* wrapper shape. `E` stays generic so the helper works
    /// with whatever error type the family's builtins return.
    pub type FcFn<E> =
        fn(Option<&mut FmgrInfo>, &mut FunctionCallInfoBaseData) -> Result<Datum, E>;

    /// Harness argument → Datum, using the same `Datum::from_*` constructors
    /// the shipped callers use. Pointers ride as `from_usize` (by-ref datum).
    pub trait ProofArg {
        fn to_datum(self) -> Datum;
    }

    macro_rules! proof_arg {
        ($($ty:ty => $ctor:ident;)*) => {$(
            impl ProofArg for $ty {
                fn to_datum(self) -> Datum { Datum::$ctor(self) }
            }
        )*};
    }

    proof_arg! {
        bool  => from_bool;
        i8    => from_char;   // SQL "char": the C fmgr convention
        i16   => from_i16;
        i32   => from_i32;
        i64   => from_i64;
        u8    => from_u8;
        u16   => from_u16;
        u32   => from_u32;
        u64   => from_u64;
        usize => from_usize;
    }

    impl<T> ProofArg for *const T {
        fn to_datum(self) -> Datum {
            Datum::from_usize(self as usize)
        }
    }

    impl<T> ProofArg for *mut T {
        fn to_datum(self) -> Datum {
            Datum::from_usize(self as usize)
        }
    }

    impl ProofArg for Datum {
        fn to_datum(self) -> Datum {
            self
        }
    }

    /// A real N-arg fcinfo frame with all args non-null.
    pub fn fci<const N: usize>(args: [Datum; N]) -> LocalFcinfo<N> {
        let mut f = LocalFcinfo::<N>::new(0);
        for (slot, d) in f.args.iter_mut().zip(args) {
            *slot = NullableDatum::value(d);
        }
        f
    }

    /// Run a shipped fc_* wrapper on an N-arg frame, returning the raw
    /// result (fallible families adjudicate the Err arm; infallible ones use
    /// [`call_ok`]).
    pub fn call<const N: usize, E>(fc: FcFn<E>, args: [Datum; N]) -> Result<Datum, E> {
        let mut f = fci(args);
        fc(None, &mut f)
    }

    /// [`call`] for wrappers whose Err arm is statically dead (comparators
    /// etc.) — panics if the "infallible" wrapper errors, which would itself
    /// be a finding.
    pub fn call_ok<const N: usize, E>(fc: FcFn<E>, args: [Datum; N]) -> Datum {
        match call(fc, args) {
            Ok(d) => d,
            Err(_) => panic!("infallible fc wrapper errored"),
        }
    }

    /// 1-arg convenience: `call1(fc, a)`.
    pub fn call1<A: ProofArg, E>(fc: FcFn<E>, a: A) -> Result<Datum, E> {
        call(fc, [a.to_datum()])
    }

    pub fn call1_ok<A: ProofArg, E>(fc: FcFn<E>, a: A) -> Datum {
        call_ok(fc, [a.to_datum()])
    }

    /// 2-arg convenience: `call2(fc, a, b)` — the dominant comparator shape.
    pub fn call2<A: ProofArg, B: ProofArg, E>(fc: FcFn<E>, a: A, b: B) -> Result<Datum, E> {
        call(fc, [a.to_datum(), b.to_datum()])
    }

    pub fn call2_ok<A: ProofArg, B: ProofArg, E>(fc: FcFn<E>, a: A, b: B) -> Datum {
        call_ok(fc, [a.to_datum(), b.to_datum()])
    }
}

/// Harness generator for the dominant shape: a 2-arg shipped fc_* wrapper vs
/// a C counterpart taking the two raw values, outputs compared through a
/// Datum extractor. Expand inside a `#[cfg(kani)]` module:
///
/// ```ignore
/// proof_support::eq_op2! {
///     eq_cash_eq: adt_cash::builtins::fc_cash_eq, pg_cash_eq, i64, as_bool as std::os::raw::c_int;
///     eq_cash_cmp: adt_cash::builtins::fc_cash_cmp, pg_cash_cmp, i64, as_i32 as i32;
/// }
/// ```
///
/// Both symbolic args get the full domain of `$ty`. Families needing fences,
/// mixed arg types, or by-ref images should hand-write the harness instead —
/// don't force the macro.
#[macro_export]
macro_rules! eq_op2 {
    ($($h:ident: $fc:path, $pg:path, $ty:ty, $extract:ident as $cast:ty;)*) => {$(
        #[kani::proof]
        fn $h() {
            let a: $ty = kani::any();
            let b: $ty = kani::any();
            let r = $crate::call2_ok($fc, a, b);
            let c = unsafe { $pg(a, b) };
            assert!(r.$extract() as $cast == c);
        }
    )*};
}

#[cfg(feature = "mcx-stubs")]
pub mod mcx_stubs {
    //! Mcx allocation stubs: a fixed static-buffer bump allocator replacing
    //! the arena machinery, for proving cores whose signatures drag in
    //! `PgVec`/`Mcx` (the measured hex-family symex wall).
    //!
    //! SOUNDNESS CONTRACT: the allocation STRATEGY is not part of any
    //! equivalence claim — the code under proof only requires that an
    //! allocation request yields a suitably sized/aligned, otherwise-unused
    //! block (or fails). This stub set satisfies that contract from a static
    //! buffer instead of the arena; the shipped code's writes/reads through
    //! the returned memory, lengths, and values all stay in the theorem.
    //! What LEAVES the proof: the arena implementation itself (block sizing,
    //! accounting, recycling) and real OOM behavior (the stub never reports
    //! exhaustion within `PROOF_HEAP_CAP`; harnesses must keep total
    //! allocation under the cap). Ledger wording: "modulo static-buffer
    //! allocator model (allocation strategy out of scope)".
    //!
    //! Apply to a harness (runs need `-Z stubbing`):
    //! ```ignore
    //! #[kani::stub(mcx::vec_with_capacity_in, proof_support::mcx_stubs::stub_vec_with_capacity_in)]
    //! ```
    //! Constraint for `stub_vec_with_capacity_in`: the harness's context must
    //! be a bump-family backend (`MemoryContext::new_bump` etc.), whose
    //! `deallocate` is a no-op — the Vec's drop hands the static-buffer
    //! pointer back to the real `Mcx`, which must not try to free it. Keep
    //! pushes within the requested capacity so no regrow path re-enters the
    //! real allocator.

    use allocator_api2::alloc::AllocError;
    use core::alloc::Layout;
    use core::ptr::NonNull;
    use core::sync::atomic::{AtomicUsize, Ordering};

    // tiny-proof-heap: 2 KiB cap for harnesses whose CNF walls on the
    // bit-blasted heap array (each SSA version of the backing array costs
    // CAP*8 SAT variables; brin-minmax measured 64 KiB -> >6 GiB, 2 KiB fits).
    #[cfg(feature = "tiny-proof-heap")]
    pub const PROOF_HEAP_CAP: usize = 1 << 11;
    #[cfg(not(feature = "tiny-proof-heap"))]
    pub const PROOF_HEAP_CAP: usize = 1 << 16;

    #[repr(align(64))]
    struct ProofHeap([u8; PROOF_HEAP_CAP]);

    static mut PROOF_HEAP: ProofHeap = ProofHeap([0; PROOF_HEAP_CAP]);
    static PROOF_HEAP_NEXT: AtomicUsize = AtomicUsize::new(0);

    /// Bump-allocate `layout` from the static proof heap.
    pub fn proof_heap_alloc(layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let size = layout.size().max(1); // keep ZSTs distinct + non-null
        let align = layout.align();
        let cur = PROOF_HEAP_NEXT.load(Ordering::Relaxed);
        let start = (cur + align - 1) & !(align - 1);
        let end = start.checked_add(size).ok_or(AllocError)?;
        if end > PROOF_HEAP_CAP {
            return Err(AllocError);
        }
        PROOF_HEAP_NEXT.store(end, Ordering::Relaxed);
        // SAFETY: start..end lies inside PROOF_HEAP; single-threaded under
        // Kani, and the bump cursor hands each range out exactly once.
        let p = unsafe { NonNull::new_unchecked((&raw mut PROOF_HEAP.0[0]).add(start)) };
        Ok(NonNull::slice_from_raw_parts(p, size))
    }

    /// Stub for `<mcx::Mcx as Allocator>::allocate` (preferred, if the Kani
    /// stub resolver accepts the trait-impl path): every context backend is
    /// replaced by the static bump above.
    pub fn stub_mcx_allocate<'mcx>(
        _m: &mcx::Mcx<'mcx>,
        layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError>
    where
        'mcx: 'mcx, // force the lifetime early-bound to match the impl's generics
    {
        proof_heap_alloc(layout)
    }

    /// Stub for `<mcx::Mcx as Allocator>::deallocate`: leak (bump.c stance).
    pub unsafe fn stub_mcx_deallocate<'mcx>(_m: &mcx::Mcx<'mcx>, _ptr: NonNull<u8>, _layout: Layout)
    where
        'mcx: 'mcx,
    {
    }

    /// Stub for `<mcx::Mcx as Allocator>::grow`: fresh block + copy.
    pub unsafe fn stub_mcx_grow<'mcx>(
        _m: &mcx::Mcx<'mcx>,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError>
    where
        'mcx: 'mcx,
    {
        let new = proof_heap_alloc(new_layout)?;
        // SAFETY: old block holds old_layout.size() readable bytes; regions
        // are disjoint (bump never re-issues).
        unsafe {
            core::ptr::copy_nonoverlapping(
                ptr.as_ptr(),
                new.cast::<u8>().as_ptr(),
                old_layout.size(),
            );
        }
        Ok(new)
    }

    /// Stub for `mcx::vec_with_capacity_in` (free-fn fallback if trait-impl
    /// stubbing is unavailable): a `PgVec` whose buffer comes from the proof
    /// heap, carrying the harness's real `Mcx` handle. See the module doc
    /// for the bump-backend constraint on drop.
    pub fn stub_vec_with_capacity_in<'mcx, T>(
        mcx: mcx::Mcx<'mcx>,
        cap: usize,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, T>> {
        let layout = Layout::array::<T>(cap).map_err(|_| mcx.oom(cap))?;
        let p = proof_heap_alloc(layout).map_err(|_| mcx.oom(layout.size()))?;
        // SAFETY: p is a fresh, exclusive, suitably aligned block of
        // cap * size_of::<T>() bytes; len 0 <= cap. Deallocation via the
        // carried Mcx is a no-op by the bump-backend constraint above.
        Ok(unsafe {
            mcx::PgVec::from_raw_parts_in(p.cast::<T>().as_ptr(), 0, cap, mcx)
        })
    }
}
