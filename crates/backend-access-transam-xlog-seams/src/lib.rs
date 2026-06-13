//! Inward seam declarations for `backend-access-transam-xlog` (`xlog.c`): the
//! xlog functions a not-yet-ported cycle partner will call across a dependency
//! cycle. The owner (`backend-access-transam-xlog`) installs each from its
//! `init_seams()`.
//!
//! On the current frontier no ported consumer reaches an xlog function across a
//! cycle: the WAL-engine entry points are still the deferred shmem driver, and
//! the one inward redo entry (`xlog_redo`) takes the `XLogReaderState` record
//! handle owned by the not-yet-ported `xlogreader`/`xlogrecovery` units — its
//! declaration lands together with that handle type, so this crate has no seams
//! yet (the empty companion the recovery/insert ports will extend).

#![allow(non_snake_case)]
