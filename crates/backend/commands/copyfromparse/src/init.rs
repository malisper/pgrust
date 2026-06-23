//! Seam installation for `backend-commands-copyfromparse`.
//!
//! This crate is the COPY FROM input *parser*. Its cross-subsystem boundary
//! points are reads/services off the `CopyFromStateData` that `copyfrom.c`
//! owns; those seams live in `backend-commands-copyfrom-seams` and are
//! installed by the `copyfrom.c` owner when it lands, not here (the parser does
//! not own them).
//!
//! No crate calls *into* the parser across a dependency cycle yet — `copyfrom.c`
//! (its only caller) will depend on it directly once ported — so the parser
//! declares no inward seams and installs nothing. The function is present and
//! wired into `seams-init` so the aggregator pattern holds uniformly; it
//! becomes non-empty only if a future cyclic caller forces a
//! `backend-commands-copyfromparse-seams` crate.
pub fn init_seams() {}
