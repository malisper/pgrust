//! The sqe production-engine shell (P2-1, production-plan §P2 + §2):
//! dispatch slot, typed refusal lattice, census surface, and the typed
//! DestReceiver seam. R1 doctrine: columnar tables are served by the sqe
//! engine or fail with a typed censused ERROR — no fallback, ever.

pub(crate) mod fixpoint;
pub(crate) mod heap;
pub(crate) mod heapjoin;
pub(crate) mod memo;
pub(crate) mod refusal;
pub(crate) mod seam;
pub(crate) mod spillstore;
pub(crate) mod stat;
pub(crate) mod stmt;
pub(crate) mod valuesbank;
