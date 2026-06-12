//! `CommandDest` (`tcop/dest.h`). Values must match the C enum ordering.

#![allow(non_upper_case_globals)]

pub type CommandDest = u32;

/// Results are discarded.
pub const DestNone: CommandDest = 0;
/// Results go to debugging output.
pub const DestDebug: CommandDest = 1;
/// Results sent to frontend process.
pub const DestRemote: CommandDest = 2;
/// Sent to frontend, in Execute command.
pub const DestRemoteExecute: CommandDest = 3;
/// Sent to frontend, with no catalog access.
pub const DestRemoteSimple: CommandDest = 4;
/// Results sent to SPI manager.
pub const DestSPI: CommandDest = 5;
/// Results sent to Tuplestore.
pub const DestTuplestore: CommandDest = 6;
/// Results sent to relation (SELECT INTO).
pub const DestIntoRel: CommandDest = 7;
/// Results sent to COPY TO code.
pub const DestCopyOut: CommandDest = 8;
/// Results sent to SQL-language func mgr.
pub const DestSQLFunction: CommandDest = 9;
/// Results sent to transient relation.
pub const DestTransientRel: CommandDest = 10;
/// Results sent to tuple queue.
pub const DestTupleQueue: CommandDest = 11;
/// Results are serialized and discarded (EXPLAIN SERIALIZE).
pub const DestExplainSerialize: CommandDest = 12;
