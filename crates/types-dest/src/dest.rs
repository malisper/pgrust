//! `CommandDest` (`tcop/dest.h`). Discriminants match the C enum ordering.

/// `typedef enum { DestNone, ... } CommandDest` (`tcop/dest.h`).
///
/// Only `None`, `Debug`, and `Remote` are legal for the `whereToSendOutput`
/// global; the other values are per-command destinations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum CommandDest {
    /// Results are discarded.
    None = 0,
    /// Results go to debugging output.
    Debug,
    /// Results sent to frontend process.
    Remote,
    /// Sent to frontend, in Execute command.
    RemoteExecute,
    /// Sent to frontend, with no catalog access.
    RemoteSimple,
    /// Results sent to SPI manager.
    Spi,
    /// Results sent to Tuplestore.
    Tuplestore,
    /// Results sent to relation (SELECT INTO).
    IntoRel,
    /// Results sent to COPY TO code.
    CopyOut,
    /// Results sent to SQL-language func mgr.
    SqlFunction,
    /// Results sent to transient relation.
    TransientRel,
    /// Results sent to tuple queue.
    TupleQueue,
    /// Results are serialized and discarded (EXPLAIN SERIALIZE).
    ExplainSerialize,
}
