#![no_std]

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum CommandDest {
    None = 0,
    Debug,
    Remote,
    RemoteExecute,
    RemoteSimple,
    Spi,
    Tuplestore,
    IntoRel,
    CopyOut,
    SqlFunction,
    TransientRel,
    TupleQueue,
    ExplainSerialize,
}
