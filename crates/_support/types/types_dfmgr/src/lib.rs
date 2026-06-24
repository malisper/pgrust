//! Dynamic-loader vocabulary for `backend-utils-fmgr-dfmgr`
//! (`src/backend/utils/fmgr/dfmgr.c`).
//!
//! The owned carriers the dynamic loader and its OS/path seams exchange: the
//! magic block (`Pg_magic_struct`), the loaded-files-list entry
//! (`DynamicFileList` → [`LoadedModule`]), the `stat`-derived file identity
//! ([`FileIdentity`], `SAME_INODE`), the opaque OS handle token
//! ([`LibraryHandle`]), the open-outcome carrier ([`LibraryOpen`]), and the
//! `get_loaded_module_details` out-parameter trio ([`LoadedModuleDetails`]).
//!
//! The opaque OS library handle (`void *handle` of `DynamicFileList`) never
//! crosses the idiomatic surface as a pointer: it is an integer token the
//! loader runtime maps to/from its real `dlopen` handle.

#![no_std]
#![allow(non_camel_case_types)]

extern crate alloc;

use alloc::string::String;

// On wasm `libc` does not export `dev_t`/`ino_t` (no POSIX stat layer). The
// dynamic loader is inert in single-user wasm (no `dlopen`), but the
// `FileIdentity` carrier still references these widths; provide local aliases
// matching the 64-bit Linux `struct stat` field widths.
#[cfg(target_family = "wasm")]
mod libc {
    pub type dev_t = u64;
    pub type ino_t = u64;
}

use ::types_core::fmgr::PgAbiValues;

/// `Pg_magic_struct` (`fmgr.h`) — a module's magic block. `len` is
/// `sizeof(Pg_magic_struct)` in C; `name`/`version` are `NULL` unless the
/// module used `PG_MODULE_MAGIC_EXT`.
#[derive(Clone, Debug)]
pub struct Pg_magic_struct {
    /// `int len` — the on-disk `sizeof(Pg_magic_struct)` the module baked in.
    pub len: i32,
    /// `Pg_abi_values abi_fields`.
    pub abi_fields: PgAbiValues,
    /// `const char *name` — optional module name (`NULL` → `None`).
    pub name: Option<String>,
    /// `const char *version` — optional module version (`NULL` → `None`).
    pub version: Option<String>,
}

/// Opaque handle for a loaded shared library — the idiomatic stand-in for the
/// `void *handle` returned by `dlopen` and stored in `DynamicFileList.handle`.
/// An integer token, not a pointer: the loader runtime owns the real `dlopen`
/// handle and maps it to/from this token. Equality compares the token,
/// matching the C pointer identity for "the same loaded library".
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LibraryHandle(pub u64);

/// File identity used to detect "same file, different path" (symlink / hard
/// link) — the `SAME_INODE` macro. Mirrors the `device`/`inode` fields of
/// `DynamicFileList`, populated from `struct stat`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileIdentity {
    /// `struct stat.st_dev` — the device the file is on.
    pub device: libc::dev_t,
    /// `struct stat.st_ino` — the file's inode number.
    pub inode: libc::ino_t,
}

/// One entry of the dynamically-loaded-files list (`DynamicFileList`): an
/// owned filename, the file identity, the OS handle token, and the module's
/// magic block. A module with no magic block is rejected before it is linked
/// into the list (`internal_load_library`), so `magic` is always present.
#[derive(Clone, Debug)]
pub struct LoadedModule {
    /// `struct stat`-derived identity (`device`, `inode`).
    pub identity: FileIdentity,
    /// `void *handle` — the `dlopen` handle token.
    pub handle: LibraryHandle,
    /// `const Pg_magic_struct *magic` — the module's magic block.
    pub magic: Pg_magic_struct,
    /// `char filename[]` — full pathname of the file (the list key).
    pub filename: String,
}

/// The trio `get_loaded_module_details` writes through its out-parameters:
/// `*library_path`, `*module_name`, `*module_version`. The latter two are
/// `Option` because `magic->name` / `magic->version` may be `NULL` in C.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedModuleDetails {
    /// `*library_path` — `dfptr->filename`.
    pub library_path: String,
    /// `*module_name` — `dfptr->magic->name` (may be `NULL` → `None`).
    pub module_name: Option<String>,
    /// `*module_version` — `dfptr->magic->version` (may be `NULL` → `None`).
    pub module_version: Option<String>,
}

/// Outcome of opening a candidate library file (`internal_load_library`):
/// either the OS handle plus its magic block, or "no magic block found" (the
/// caller then `dlclose`s and issues the "missing magic block" error,
/// mirroring the C control flow).
#[derive(Clone, Debug)]
pub enum LibraryOpen {
    /// Library opened and a magic block (`Pg_magic_func`) was found.
    WithMagic {
        handle: LibraryHandle,
        magic: Pg_magic_struct,
    },
    /// Library opened but `dlsym(handle, "Pg_magic_func")` returned `NULL`.
    /// The handle is carried so the loader can `dlclose` it before erroring.
    MissingMagic { handle: LibraryHandle },
}
