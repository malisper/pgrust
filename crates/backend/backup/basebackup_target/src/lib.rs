//! Port of PostgreSQL's `basebackup_target` (`src/backend/backup/
//! basebackup_target.c`).
//!
//! Base backups can be "targeted", which means that they can be sent somewhere
//! other than to the client which requested the backup. Furthermore, new
//! targets can be defined by extensions. This crate contains the code to
//! support that functionality.
//!
//! Every function defined by `basebackup_target.c` is ported 1:1 here:
//! [`BaseBackupAddTarget`], [`BaseBackupGetTargetHandle`], [`BaseBackupGetSink`],
//! `initialize_target_list`, `blackhole_get_sink`, `server_get_sink`,
//! `reject_target_detail`, and `server_check_detail`. The single
//! genuinely-external call (`bbsink_server_new`, from `basebackup_server.c`) is
//! a direct call into the landed [`server`] crate (no cycle).
//!
//! # The owned model
//!
//! The C code is a registry of *target types*, each a pair of raw function
//! pointers:
//!
//! ```c
//! typedef struct BaseBackupTargetType
//! {
//!     char       *name;
//!     void       *(*check_detail) (char *, char *);
//!     bbsink     *(*get_sink) (bbsink *, void *);
//! } BaseBackupTargetType;
//! ```
//!
//! The function-pointer pair `(check_detail, get_sink)` becomes the
//! [`BaseBackupTarget`] trait, the per-target opaque `void *detail_arg`
//! (whatever `check_detail` returns and `get_sink` later consumes) becomes the
//! owned [`TargetDetail`] value, and the C `bbsink *` chain is the owned
//! [`Box<Bbsink>`](sink::Bbsink) chain from
//! `backend-backup-sink`. There is no raw `void *`, no function pointer, and no
//! `extern "C"`.
//!
//! Faithfulness notes:
//!  * `BaseBackupTargetTypeList` is a process-global `List *` in C; a PostgreSQL
//!    backend is a single-threaded process, so it is modeled here as a
//!    `thread_local!` [`RefCell`] holding the registry [`Vec`]. NIL (the empty
//!    initial list) is the empty `Vec`; the deferred `initialize_target_list`
//!    that loads the two builtin targets on first use is preserved exactly.
//!  * The C "update in place if the name already exists, else append" behavior
//!    of [`BaseBackupAddTarget`] is preserved.
//!  * The C `palloc` into `TopMemoryContext` (so the registry survives for the
//!    life of the backend) corresponds to the registry `Vec` living for the
//!    life of the thread-local; the owned values carry their own storage.
//!  * The `BaseBackupTargetHandle` produced by [`BaseBackupGetTargetHandle`]
//!    carries the resolved target's name (the analog of the C `handle->type`
//!    back-pointer into the registry) plus the owned detail; [`BaseBackupGetSink`]
//!    re-resolves the registered target by that name and dispatches to its
//!    `get_sink`, which is exactly what `handle->type->get_sink(...)` does in C.
//!  * The repo's `bbsink_server_new` allocates its sink into a memory context,
//!    so [`BaseBackupTarget::get_sink`] / [`BaseBackupGetSink`] thread the
//!    surrounding [`Mcx`] (the `palloc` analog) through to it. The C signature
//!    has no such argument because C uses the ambient `CurrentMemoryContext`.

#![allow(non_snake_case)]

use std::cell::RefCell;

use sink::Bbsink;
use server::bbsink_server_new;
use utils_error::ereport;
use mcx::Mcx;
use types_error::{
    ErrorLocation, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR, ERROR,
};

/// Source file name reported in `ereport` locations (matches the C `__FILE__`).
const SRCFILE: &str = "src/backend/backup/basebackup_target.c";

/// The opaque per-target detail produced by a target's `check_detail` and later
/// consumed by its `get_sink` — the owned replacement for C's `void *detail_arg`.
///
/// In C this is an untyped `void *` that lets `check_detail` and `get_sink`
/// communicate. The two builtin targets use it as follows:
///  * `blackhole` (`reject_target_detail`) never accepts a detail and returns
///    `NULL` → [`TargetDetail::None`].
///  * `server` (`server_check_detail`) returns the (required) target detail
///    string unchanged → [`TargetDetail::Server`] carrying the directory name.
///
/// An extension target may define its own detail; rather than expose a raw
/// pointer, an extension carries arbitrary owned state in [`TargetDetail::Other`]
/// as a boxed `Any`.
pub enum TargetDetail {
    /// No detail (C `NULL`); used by the `blackhole` target.
    None,
    /// A validated server-side directory name; used by the `server` target.
    Server(String),
    /// Arbitrary owned detail defined by an extension target.
    Other(Box<dyn std::any::Any>),
}

/// A base backup target type.
///
/// This trait is the idiomatic replacement for the C function-pointer pair
/// `(check_detail, get_sink)` carried by `struct BaseBackupTargetType`. The C
/// `name` field lives alongside the trait object in [`BaseBackupTargetType`].
pub trait BaseBackupTarget {
    /// Validate the `target_detail` for this target and produce the opaque
    /// detail that will later be handed to [`BaseBackupTarget::get_sink`].
    ///
    /// C: `void *(*check_detail) (char *target, char *target_detail)`. The
    /// `target` name and the (optional) `target_detail` are passed through as in
    /// C; `target_detail` is `None` when the client supplied no detail.
    fn check_detail(&self, target: &str, target_detail: Option<&str>) -> PgResult<TargetDetail>;

    /// Construct a [`Bbsink`] that implements this backup target, wrapping the
    /// successor sink and consuming the detail produced by
    /// [`BaseBackupTarget::check_detail`].
    ///
    /// C: `bbsink *(*get_sink) (bbsink *next_sink, void *detail_arg)`. `mcx` is
    /// the surrounding memory context into which a sink constructed here is
    /// allocated (the C ambient `CurrentMemoryContext`).
    fn get_sink<'mcx>(
        &self,
        mcx: Mcx<'mcx>,
        next_sink: Box<Bbsink<'mcx>>,
        detail_arg: TargetDetail,
    ) -> PgResult<Box<Bbsink<'mcx>>>;
}

/// `typedef struct BaseBackupTargetType` — the registered name plus its target.
///
/// The C struct's three fields (`name`, `check_detail`, `get_sink`) become the
/// owned `name` plus the [`BaseBackupTarget`] trait object (which supplies the
/// two callbacks).
pub struct BaseBackupTargetType {
    /// The target's name (C `char *name`).
    pub name: String,
    /// The target's callbacks (C `check_detail` + `get_sink`).
    pub target: Box<dyn BaseBackupTarget>,
}

/// `struct BaseBackupTargetHandle`.
///
/// In C this is `{ BaseBackupTargetType *type; void *detail_arg; }`. The raw
/// back-pointer into the registry is modeled by the resolved target's `name`
/// (which [`BaseBackupGetSink`] re-resolves), and `detail_arg` is the owned
/// [`TargetDetail`].
pub struct BaseBackupTargetHandle {
    /// The name of the resolved registered target (analog of C `handle->type`).
    pub type_name: String,
    /// The opaque detail produced by the target's `check_detail` (C
    /// `handle->detail_arg`).
    pub detail_arg: TargetDetail,
}

thread_local! {
    /// `static List *BaseBackupTargetTypeList = NIL;`
    ///
    /// A PostgreSQL backend is a single-threaded process; this thread-local
    /// registry models the process global faithfully. The empty `Vec` is NIL;
    /// it is populated on first use by `initialize_target_list`, exactly as in
    /// C, and survives for the life of the backend (the C `TopMemoryContext`
    /// lifetime).
    static BASE_BACKUP_TARGET_TYPE_LIST: RefCell<Vec<BaseBackupTargetType>> =
        const { RefCell::new(Vec::new()) };
}

/// Add a new base backup target type.
///
/// This is intended for use by server extensions.
///
/// C: `void BaseBackupAddTarget(char *name, check_detail, get_sink)`.
pub fn BaseBackupAddTarget(name: &str, target: Box<dyn BaseBackupTarget>) {
    // If the target list is not yet initialized, do that first.
    if BASE_BACKUP_TARGET_TYPE_LIST.with(|l| l.borrow().is_empty()) {
        initialize_target_list();
    }

    BASE_BACKUP_TARGET_TYPE_LIST.with(|l| {
        let mut list = l.borrow_mut();

        // Search the target type list for an existing entry with this name.
        for ttype in list.iter_mut() {
            if ttype.name == name {
                // We found one, so update it.
                //
                // It is probably not a great idea to call BaseBackupAddTarget
                // for the same name multiple times, but if it happens, this
                // seems like the sanest behavior.
                ttype.target = target;
                return;
            }
        }

        // We use TopMemoryContext for allocations here to make sure that the
        // data we need doesn't vanish under us; that's also why we copy the
        // target name into a newly-allocated chunk of memory. In the owned
        // model the registry `Vec` lives for the life of the backend and the
        // pushed entry owns its copied name, so the same lifetime holds.
        list.push(BaseBackupTargetType {
            name: name.to_string(),
            target,
        });
    });
}

/// Look up a base backup target and validate the `target_detail`.
///
/// Extensions that define new backup targets will probably define a new type of
/// bbsink to match. Validation of the `target_detail` can be performed either in
/// the `check_detail` routine called here, or in the bbsink constructor, which
/// will be called from [`BaseBackupGetSink`]. It's mostly a matter of taste, but
/// the `check_detail` function runs somewhat earlier.
///
/// C: `BaseBackupTargetHandle *BaseBackupGetTargetHandle(char *target, char *target_detail)`.
pub fn BaseBackupGetTargetHandle(
    target: &str,
    target_detail: Option<&str>,
) -> PgResult<BaseBackupTargetHandle> {
    // If the target list is not yet initialized, do that first.
    if BASE_BACKUP_TARGET_TYPE_LIST.with(|l| l.borrow().is_empty()) {
        initialize_target_list();
    }

    // Search the target type list for a match.
    let detail = BASE_BACKUP_TARGET_TYPE_LIST.with(|l| {
        let list = l.borrow();
        for ttype in list.iter() {
            if ttype.name == target {
                // Found the target.
                //
                // The C code palloc's the handle, sets handle->type to the
                // registry entry, and stores handle->detail_arg =
                // ttype->check_detail(target, target_detail). Here we run
                // check_detail and return Some(detail); the handle is built
                // below (carrying the target name as the type back-pointer).
                return Ok(Some(ttype.target.check_detail(target, target_detail)?));
            }
        }
        Ok(None)
    })?;

    match detail {
        Some(detail_arg) => Ok(BaseBackupTargetHandle {
            type_name: target.to_string(),
            detail_arg,
        }),
        // Did not find the target.
        None => ereport_unrecognized_target(target),
    }
}

/// Construct a [`Bbsink`] that will implement the backup target.
///
/// The `get_sink` function does all the real work, so all we have to do here is
/// call it with the correct arguments. Whatever the `check_detail` function
/// returned is here passed through to the `get_sink` function. This lets those
/// two functions communicate with each other, if they wish. If not, the
/// `check_detail` function can simply return the `target_detail` and let the
/// `get_sink` function take it from there.
///
/// C: `bbsink *BaseBackupGetSink(BaseBackupTargetHandle *handle, bbsink *next_sink)`.
///
/// The C `handle->type->get_sink(next_sink, handle->detail_arg)` is modeled by
/// re-resolving the registered target by `handle.type_name` (the analog of the
/// C back-pointer) and dispatching to its `get_sink`. The target was registered
/// when the handle was produced, so the lookup always succeeds; if it somehow
/// does not, that is the same "unrecognized target" error the C lookup raises.
/// `mcx` is the surrounding memory context for any sink the target constructs.
pub fn BaseBackupGetSink<'mcx>(
    mcx: Mcx<'mcx>,
    handle: BaseBackupTargetHandle,
    next_sink: Box<Bbsink<'mcx>>,
) -> PgResult<Box<Bbsink<'mcx>>> {
    let BaseBackupTargetHandle {
        type_name,
        detail_arg,
    } = handle;
    BASE_BACKUP_TARGET_TYPE_LIST.with(|l| {
        let list = l.borrow();
        for ttype in list.iter() {
            if ttype.name == type_name {
                return ttype.target.get_sink(mcx, next_sink, detail_arg);
            }
        }
        // The handle's type vanished from the registry; treat it as the same
        // unrecognized-target error the lookup path would raise.
        ereport_unrecognized_target(&type_name)
    })
}

/// Load predefined target types into `BaseBackupTargetTypeList`.
///
/// C: `static void initialize_target_list(void)`.
///
/// The C `static BaseBackupTargetType builtin_backup_targets[]` array (a
/// `{ blackhole, … }`, `{ server, … }`, `{ NULL }`-terminated table) is loaded
/// here; the NULL terminator is modeled implicitly by iterating the two real
/// builtin entries.
fn initialize_target_list() {
    BASE_BACKUP_TARGET_TYPE_LIST.with(|l| {
        let mut list = l.borrow_mut();
        list.push(BaseBackupTargetType {
            name: "blackhole".to_string(),
            target: Box::new(BlackholeTarget),
        });
        list.push(BaseBackupTargetType {
            name: "server".to_string(),
            target: Box::new(ServerTarget),
        });
    });
}

/// The builtin `blackhole` target.
///
/// `check_detail` rejects any detail (`reject_target_detail`) and `get_sink`
/// throws the data away by simply returning the successor sink unchanged
/// (`blackhole_get_sink`).
struct BlackholeTarget;

impl BaseBackupTarget for BlackholeTarget {
    /// Implement target-detail checking for a target that does not accept a
    /// detail.
    ///
    /// C: `static void *reject_target_detail(char *target, char *target_detail)`.
    fn check_detail(&self, target: &str, target_detail: Option<&str>) -> PgResult<TargetDetail> {
        if target_detail.is_some() {
            return ereport_syntax_error(
                format!("target \"{target}\" does not accept a target detail"),
                216,
                "reject_target_detail",
            );
        }

        Ok(TargetDetail::None)
    }

    /// Normally, a `get_sink` function should construct and return a new bbsink
    /// that implements the backup target, but the 'blackhole' target just throws
    /// the data away. We could implement that by adding a bbsink that does
    /// nothing but forward, but it's even cheaper to implement that by not
    /// adding a bbsink at all.
    ///
    /// C: `static bbsink *blackhole_get_sink(bbsink *next_sink, void *detail_arg)`.
    fn get_sink<'mcx>(
        &self,
        _mcx: Mcx<'mcx>,
        next_sink: Box<Bbsink<'mcx>>,
        _detail_arg: TargetDetail,
    ) -> PgResult<Box<Bbsink<'mcx>>> {
        Ok(next_sink)
    }
}

/// The builtin `server` target.
///
/// `check_detail` requires a directory-name detail (`server_check_detail`) and
/// `get_sink` builds a server-side-write sink (`server_get_sink`, which calls
/// `bbsink_server_new`).
struct ServerTarget;

impl BaseBackupTarget for ServerTarget {
    /// Implement target-detail checking for a server-side backup.
    ///
    /// `target_detail` should be the name of the directory to which the backup
    /// should be written, but we don't check that here. Rather, that check, as
    /// well as the necessary permissions checking, happens in `bbsink_server_new`.
    ///
    /// C: `static void *server_check_detail(char *target, char *target_detail)`.
    fn check_detail(&self, target: &str, target_detail: Option<&str>) -> PgResult<TargetDetail> {
        match target_detail {
            None => ereport_syntax_error(
                format!("target \"{target}\" requires a target detail"),
                235,
                "server_check_detail",
            ),
            Some(detail) => Ok(TargetDetail::Server(detail.to_string())),
        }
    }

    /// Create a bbsink implementing a server-side backup.
    ///
    /// C: `static bbsink *server_get_sink(bbsink *next_sink, void *detail_arg)`,
    /// which is `return bbsink_server_new(next_sink, detail_arg);`.
    fn get_sink<'mcx>(
        &self,
        mcx: Mcx<'mcx>,
        next_sink: Box<Bbsink<'mcx>>,
        detail_arg: TargetDetail,
    ) -> PgResult<Box<Bbsink<'mcx>>> {
        // detail_arg is the validated directory name produced by
        // server_check_detail (TargetDetail::Server).
        let pathname = match detail_arg {
            TargetDetail::Server(pathname) => pathname,
            // server_check_detail only ever yields TargetDetail::Server; any
            // other value would be a wiring bug in the registry.
            _ => unreachable!("server target detail must be a directory name"),
        };
        bbsink_server_new(mcx, next_sink, pathname)
    }
}

/// `ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg(msg)))` at the given
/// source line — always returns `Err`.
///
/// Shared by `reject_target_detail` (a detail was supplied to a target that
/// rejects details) and `server_check_detail` (a detail was required but
/// missing).
fn ereport_syntax_error<T>(msg: String, lineno: i32, funcname: &'static str) -> PgResult<T> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(msg)
        .into_error()
        .with_error_location(ErrorLocation::new(SRCFILE, lineno, funcname)))
}

/// `ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unrecognized
/// target: \"%s\"", target)))` — always returns `Err`.
///
/// Raised by [`BaseBackupGetTargetHandle`] when no registered target matches.
fn ereport_unrecognized_target<T>(target: &str) -> PgResult<T> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(format!("unrecognized target: \"{target}\""))
        .into_error()
        .with_error_location(ErrorLocation::new(SRCFILE, 144, "BaseBackupGetTargetHandle")))
}

/// This crate owns no inward seam crate (its only core consumer, `basebackup.c`,
/// calls in via a direct dependency, not across a cycle), so its installer is
/// empty. Wired into `seams-init::init_all()` for uniformity and to satisfy the
/// recurrence guard.
pub fn init_seams() {}
