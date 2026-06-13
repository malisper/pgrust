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
pub mod execstate_tags;
pub mod executor;
pub mod fmgr;
pub mod funcapi;
pub mod instrument;
pub mod nodeforeigncustom;
pub mod nodeindexscan;
pub mod nodes;
pub mod parsenodes;
pub mod pathnodes;
pub mod planstate;
pub mod primnodes;
pub mod queryenvironment;

pub use bitmapset::Bitmapset;
pub use execexpr::SubPlanState;
pub use execnodes::{
    EStateData, EcxtId, ExecProcNodeMtd, ExprContext, ExprContextCallbackFunction,
    ExprContext_CB, Opaque, ParamExecData, PlanStateData, ResultRelInfo, RriId,
    ScanDirection, ScanDirectionIsForward, ScanStateData, SlotId, T_MaterialState,
};
pub use instrument::Instrumentation;
pub use pathnodes::PathNode;
pub use executor::{TupleSlotKind, TupleTableSlot};
pub use funcapi::Tuplestorestate;
pub use nodeforeigncustom::{Material, MaterialState};
pub use parsenodes::{RTEPermissionInfo, RangeTblEntry};
pub use planstate::PlanStateNode;
pub use primnodes::{Expr, TargetEntry, Var};
pub use queryenvironment::{
    EphemeralNameRelationType, EphemeralNamedRelation, EphemeralNamedRelationData,
    EphemeralNamedRelationMetadata, EphemeralNamedRelationMetadataData, QueryEnvironment,
    ENR_NAMED_TUPLESTORE,
};
