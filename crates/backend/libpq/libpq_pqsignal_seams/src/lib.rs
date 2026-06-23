//! Seam declarations for `src/backend/libpq/pqsignal.c` — the owner of the
//! backend signal masks (`BlockSig`/`UnBlockSig`/`StartupBlockSig`).
//!
//! These wrap `sigprocmask(SIG_SETMASK, &BlockSig/&UnBlockSig, NULL)`, the
//! mask-install primitives behind `BackgroundWorkerBlockSignals` /
//! `BackgroundWorkerUnblockSignals` and the like. The owning crate
//! (`backend-libpq-pqsignal`) installs these from its `init_seams()`.

seam_core::seam!(
    /// `sigprocmask(SIG_SETMASK, &BlockSig, NULL)` — block all signals in the
    /// owner's `BlockSig` set. `BlockSig` is owned by `backend-libpq-pqsignal`
    /// (`pqinitmask`); infallible.
    pub fn block_signals()
);

seam_core::seam!(
    /// `sigprocmask(SIG_SETMASK, &UnBlockSig, NULL)` — restore the normal
    /// signal mask from the owner's `UnBlockSig` set. Infallible.
    pub fn unblock_signals()
);

seam_core::seam!(
    /// `sigaddset(&UnBlockSig, signo)` — persistently add `signo` to the
    /// owner's `UnBlockSig` set. waiteventset.c's signalfd build does this for
    /// `SIGURG` so that signal is delivered through the signalfd rather than a
    /// handler. Infallible.
    pub fn add_unblock_sig(signo: i32)
);
