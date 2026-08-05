//! Seams for `backup_copy`: the `DestRemoteSimple` result-set path + flush.

use ::types_core::Oid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultColumnType {
    Text,
    Int8,
    Oid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultColumn {
    pub name: String,
    pub typ: ResultColumnType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultValue {
    Text(String),
    Int8(i64),
    Oid(Oid),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DestReceiverHandle(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TupOutputState {
    pub dest: DestReceiverHandle,
}

seam_core::seam!(
    pub fn create_dest_remote_simple() -> DestReceiverHandle
);

seam_core::seam!(
    pub fn begin_tup_output_tupdesc(
        dest: DestReceiverHandle,
        columns: Vec<ResultColumn>,
    ) -> TupOutputState
);

seam_core::seam!(
    pub fn do_tup_output(tstate: TupOutputState, values: Vec<Option<ResultValue>>)
);

seam_core::seam!(
    pub fn end_tup_output(tstate: TupOutputState)
);

seam_core::seam!(
    // Owner: backend-libpq-pqcomm. Emits no wire bytes; flushes assembled buffer.
    pub fn pq_flush_if_writable() -> ::types_error::PgResult<i32>
);
