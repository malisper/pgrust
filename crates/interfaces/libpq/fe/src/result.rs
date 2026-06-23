//! The owned `PGresult` data model (`fe-exec.c`).
//!
//! `fe-exec.c` splits the `PGresult` into a block-arena (`PGresult_data`) plus
//! `char *` / pointer-array members. The owned-Rust model replaces the arena
//! with `Vec`/`String`/`Option<Vec<u8>>` owned fields; `PQclear` is therefore
//! ordinary `Drop`. Only the fields the seam consumers (walreceiver, ecpg
//! simple-query) read are modelled — the per-field error diagnostics arena, the
//! event/instance hooks, and the noticeHooks are not.

pub use ::types_libpqwalreceiver::ExecStatusType;

/// `PGresAttDesc` (`libpq-fe.h`) — one column's attribute descriptor, read out
/// of a RowDescription ('T') message (`getRowDescriptions`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PgResAttDesc {
    /// `name` — column name.
    pub name: String,
    /// `tableid` — source table OID (0 if not a simple column reference).
    pub tableid: u32,
    /// `columnid` — source column number.
    pub columnid: i32,
    /// `format` — 0 text, 1 binary.
    pub format: i32,
    /// `typid` — column type OID.
    pub typid: u32,
    /// `typlen` — type length.
    pub typlen: i32,
    /// `atttypmod` — type modifier.
    pub atttypmod: i32,
}

/// One field's value within a tuple row. A SQL NULL is `None`; otherwise the
/// raw (text or binary) bytes the backend sent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PgResAttValue {
    /// `value` — the field bytes, or `None` for a SQL NULL (`NULL_LEN`).
    pub value: Option<Vec<u8>>,
}

/// The owned `PGresult` (`struct pg_result`): the result of one SQL command. A
/// `TUPLES_OK` result carries `att_descs` + `tuples`; a `COMMAND_OK` result
/// carries a `cmd_status` tag; an error result carries `err_msg` (+ `sqlstate`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PGresult {
    /// `resultStatus`.
    pub result_status: ExecStatusType,
    /// `attDescs` — the column descriptors (empty for a non-tuple result).
    pub att_descs: Vec<PgResAttDesc>,
    /// `tuples` — the rows; each is one value per column.
    pub tuples: Vec<Vec<PgResAttValue>>,
    /// `cmdStatus` — the command tag (e.g. `"SELECT 5"` / `"INSERT 0 1"`).
    pub cmd_status: String,
    /// `binary` — 1 if every column is binary, else 0.
    pub binary: i32,
    /// `insert_oid` — the OID inserted by a single-row INSERT (else 0).
    pub ins_oid: u32,
    /// `errMsg` — the primary error message text (`PQresultErrorMessage`).
    pub err_msg: Option<String>,
    /// The `PG_DIAG_SQLSTATE` field of an ErrorResponse, if present
    /// (`PQresultErrorField(res, PG_DIAG_SQLSTATE)`).
    pub sqlstate: Option<String>,
}

impl PGresult {
    /// `PQmakeEmptyPGresult(conn, status)` — a result with no tuples / columns.
    pub fn make_empty(status: ExecStatusType) -> Self {
        PGresult {
            result_status: status,
            att_descs: Vec::new(),
            tuples: Vec::new(),
            cmd_status: String::new(),
            binary: 0,
            ins_oid: 0,
            err_msg: None,
            sqlstate: None,
        }
    }

    /// `PQresultStatus(res)`.
    pub fn result_status(&self) -> ExecStatusType {
        self.result_status
    }

    /// `PQntuples(res)`.
    pub fn ntuples(&self) -> i32 {
        self.tuples.len() as i32
    }

    /// `PQnfields(res)`.
    pub fn nfields(&self) -> i32 {
        self.att_descs.len() as i32
    }

    /// `PQfname(res, field_num)` — `None` for an out-of-range column.
    pub fn fname(&self, field_num: i32) -> Option<&str> {
        if field_num < 0 {
            return None;
        }
        self.att_descs
            .get(field_num as usize)
            .map(|a| a.name.as_str())
    }

    /// `PQgetvalue(res, tup_num, field_num)` — the field bytes. libpq returns a
    /// pointer to an empty string for a SQL NULL or an out-of-range cell, so we
    /// surface an empty slice in those cases (callers pair this with
    /// `PQgetisnull`).
    pub fn get_value(&self, tup_num: i32, field_num: i32) -> &[u8] {
        if tup_num < 0 || field_num < 0 {
            return &[];
        }
        match self
            .tuples
            .get(tup_num as usize)
            .and_then(|r| r.get(field_num as usize))
        {
            Some(PgResAttValue { value: Some(bytes) }) => bytes.as_slice(),
            _ => &[],
        }
    }

    /// `PQgetisnull(res, tup_num, field_num)` — true for a SQL NULL or an
    /// out-of-range cell (libpq returns 1 for out-of-range too).
    pub fn get_isnull(&self, tup_num: i32, field_num: i32) -> bool {
        if tup_num < 0 || field_num < 0 {
            return true;
        }
        match self
            .tuples
            .get(tup_num as usize)
            .and_then(|r| r.get(field_num as usize))
        {
            Some(PgResAttValue { value: Some(_) }) => false,
            _ => true,
        }
    }

    /// `PQgetlength(res, tup_num, field_num)` — the field byte length (0 for a
    /// SQL NULL / out-of-range cell).
    pub fn get_length(&self, tup_num: i32, field_num: i32) -> i32 {
        if tup_num < 0 || field_num < 0 {
            return 0;
        }
        match self
            .tuples
            .get(tup_num as usize)
            .and_then(|r| r.get(field_num as usize))
        {
            Some(PgResAttValue { value: Some(bytes) }) => bytes.len() as i32,
            _ => 0,
        }
    }
}

/// `PGTransactionStatusType` (`libpq-fe.h`) — the value `PQtransactionStatus`
/// returns, set from a ReadyForQuery ('Z') message.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PgTransactionStatusType {
    /// `PQTRANS_IDLE` — not in a transaction.
    Idle,
    /// `PQTRANS_ACTIVE` — a command is in progress (only transiently observed).
    Active,
    /// `PQTRANS_INTRANS` — in a valid transaction block.
    Intrans,
    /// `PQTRANS_INERROR` — in a failed transaction block.
    Inerror,
    /// `PQTRANS_UNKNOWN` — connection is bad.
    #[default]
    Unknown,
}
