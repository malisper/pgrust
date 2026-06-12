//! The designated node/executor knot crate (docs/types.md rule 4): plan-node,
//! plan-state, slot, and tuplestore-carrier vocabulary shared by the executor
//! node crates.
//!
//! The 49-module node/executor tangle in src-idiomatic's types crate is
//! irreducible, so its modules land here — but each is trimmed to only the
//! items the ports so far consume. Module names follow src-idiomatic.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod bitmapset;
pub mod execexpr;
pub mod execnodes;
pub mod executor;
pub mod fmgr;
pub mod funcapi;
pub mod nodeforeigncustom;
pub mod nodeindexscan;
pub mod nodes;
pub mod planstate;

pub use bitmapset::Bitmapset;
pub use execnodes::{
    EStateData, ExecProcNodeMtd, PlanStateData, ScanDirection, ScanDirectionIsForward,
    ScanStateData, SlotId,
};
pub use executor::{TupleSlotKind, TupleTableSlot};
pub use fmgr::FunctionCallInfoBaseData;
pub use funcapi::{ReturnSetInfo, Tuplestorestate};
pub use nodeforeigncustom::{Material, MaterialState};
pub use planstate::PlanStateNode;
