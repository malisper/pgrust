//! Family: **read** — `nodes/read.c`, the node-tree de-serializer entry
//! (`stringToNode`).
//!
//! `stringToNode` / `stringToNodeInternal` / `stringToNodeWithLocations` plus
//! the tokenizer (`pg_strtok`, `debackslash`, `nodeTokenType`) and the
//! dispatch entry `nodeRead`. The per-tag field readers live in the
//! `readfuncs` unit (`backend-nodes-readfuncs`, separate catalog row); this
//! family is the tokenizer + driver only.
//!
//! Owns the canonical `backend-nodes-read-seams` (`string_to_node`) — installed
//! in `init_seams()` when this family is filled. The reconstructed node tree is
//! `mcx`-allocated.
//!
//! Builds on value+core (node identity). Skeleton: the reader lands when
//! filled.

#![allow(unused)]

/// Family marker — the node reader lands here. See module docs.
pub fn read_family_unimplemented() -> ! {
    todo!("read: nodes/read.c not yet ported (decomp family)")
}
