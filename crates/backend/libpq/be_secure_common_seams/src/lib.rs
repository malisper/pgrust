//! Seam declarations for the `backend-libpq-be-secure-common` unit
//! (`libpq/be-secure-common.c`): the SSL-library-independent secure-transport
//! helpers shared by `be-secure-openssl.c` (the only consumer).
//!
//! The owning unit installs these from its `init_seams()`; until then a call
//! panics loudly.

use mcx::{Mcx, PgVec};
use ::types_error::PgResult;

seam_core::seam!(
    /// `int run_ssl_passphrase_command(const char *prompt, bool is_server_start,
    /// char *buf, int size)` (`be-secure-common.c`) — run the
    /// `ssl_passphrase_command` GUC (with `%p` replaced by `prompt`), reading the
    /// passphrase from the command's pipe. `is_server_start` selects the loglevel
    /// of error reports (`ERROR` vs `LOG`).
    ///
    /// C writes the passphrase into the caller's `buf` of capacity `size` and
    /// returns its length. The owned form returns the passphrase bytes
    /// (`buf.len()` is the C return value), already stripped of a trailing
    /// CR/LF and capped at `size - 1` bytes. On a failure reported at `LOG`
    /// (i.e. `!is_server_start`), an empty `PgVec` is returned (C's
    /// fall-through-with-`len == 0`); a failure at `ERROR` propagates as `Err`.
    pub fn run_ssl_passphrase_command<'mcx>(
        mcx: Mcx<'mcx>,
        prompt: &str,
        is_server_start: bool,
        size: i32,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `bool check_ssl_key_file_permissions(const char *ssl_key_file,
    /// bool isServerStart)` (`be-secure-common.c`) — `stat` the key file and
    /// enforce the ownership/mode rules (regular file; owned by us or root; no
    /// group/world access beyond `0600`/`0640`). `isServerStart` selects the
    /// loglevel (`FATAL` vs `LOG`). Returns `true` if the file is acceptable,
    /// `false` after emitting a `LOG`-level report; a `FATAL` report (server
    /// start) propagates as `Err`.
    pub fn check_ssl_key_file_permissions(
        ssl_key_file: &str,
        is_server_start: bool,
    ) -> PgResult<bool>
);
