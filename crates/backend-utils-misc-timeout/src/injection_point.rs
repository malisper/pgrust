//! Port of `src/backend/utils/misc/injection_point.c`.
//!
//! `USE_INJECTION_POINTS` is a build-time option that is **off** on this target,
//! so the active translation unit is the `#else` half: shmem sizing is zero,
//! init is a no-op, and every other operation `elog(ERROR, ...)`s with
//! "(I|i)njection points are not supported by this build". The shared-memory
//! generation-counter machinery in the `#ifdef USE_INJECTION_POINTS` branch is
//! not part of this build and is therefore not compiled here; enabling injection
//! points would land that branch (LWLock + shmem + dynamic loading) separately.
//!
//! This crate owns the `backend-storage-ipc-injection-point-seams` declarations
//! that ipci.c consumes (`InjectionPointShmemSize`/`InjectionPointShmemInit`)
//! and installs them from `init_seams`.

use backend_utils_error::{elog, PgResult};
use types_core::Size;
use types_error::ERROR;

// C: lowercase "injection" only in InjectionPointAttach; uppercase elsewhere.
const UNSUPPORTED_LOWER: &str = "injection points are not supported by this build";
const UNSUPPORTED_UPPER: &str = "Injection points are not supported by this build";

/// `InjectionPointShmemSize()` — `#else`: zero bytes when disabled.
pub fn injection_point_shmem_size() -> PgResult<Size> {
    Ok(0)
}

/// `InjectionPointShmemInit()` — `#else`: no shared memory to allocate.
pub fn injection_point_shmem_init() -> PgResult<()> {
    Ok(())
}

/// `InjectionPointAttach` — `#else`: unsupported in this build.
pub fn injection_point_attach(
    _name: &str,
    _library: &str,
    _function: &str,
    _private_data: Option<&[u8]>,
) -> PgResult<()> {
    elog(ERROR, UNSUPPORTED_LOWER)
}

/// `InjectionPointDetach` — `#else`: unsupported. The C stub `return true` only
/// silences the compiler after `elog(ERROR)` `longjmp`s, so the success value is
/// never observed; the `bool` payload is kept for shape parity.
pub fn injection_point_detach(_name: &str) -> PgResult<bool> {
    elog(ERROR, UNSUPPORTED_UPPER).map(|()| true)
}

/// `InjectionPointLoad` — `#else`: unsupported in this build.
pub fn injection_point_load(_name: &str) -> PgResult<()> {
    elog(ERROR, UNSUPPORTED_UPPER)
}

/// `InjectionPointRun` — `#else`: unsupported in this build.
pub fn injection_point_run(_name: &str, _arg: Option<&mut [u8]>) -> PgResult<()> {
    elog(ERROR, UNSUPPORTED_UPPER)
}

/// The `INJECTION_POINT(name, arg)` *macro* itself (`utils/injection_point.h`).
/// When `USE_INJECTION_POINTS` is **off** — this build — the macro expands to
/// `((void) name)`: a pure no-op that never calls `InjectionPointRun`. Call
/// sites reach it through the `domains_seams::injection_point` seam.
pub fn injection_point_macro(_name: &str) {
    // ((void) name)
}

/// `InjectionPointCached` — `#else`: unsupported in this build.
pub fn injection_point_cached(_name: &str, _arg: Option<&mut [u8]>) -> PgResult<()> {
    elog(ERROR, UNSUPPORTED_UPPER)
}

/// `IsInjectionPointAttached` — `#else`: unsupported (the C stub's
/// `return false` is unreachable past the `elog(ERROR)`).
pub fn is_injection_point_attached(_name: &str) -> PgResult<bool> {
    elog(ERROR, UNSUPPORTED_UPPER).map(|()| false)
}
