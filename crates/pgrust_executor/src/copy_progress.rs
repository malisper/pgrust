use std::cell::RefCell;

use pgrust_nodes::Value;

#[derive(Debug, Clone)]
pub struct CopyProgressSnapshot {
    pub pid: i32,
    pub datid: u32,
    pub datname: String,
    pub relid: u32,
    pub command: &'static str,
    pub copy_type: &'static str,
    pub bytes_processed: i64,
    pub bytes_total: i64,
    pub tuples_processed: i64,
    pub tuples_excluded: i64,
    pub tuples_skipped: i64,
}

thread_local! {
    static CURRENT_COPY_PROGRESS: RefCell<Option<CopyProgressSnapshot>> = const { RefCell::new(None) };
}

pub struct CopyProgressGuard;

impl Drop for CopyProgressGuard {
    fn drop(&mut self) {
        CURRENT_COPY_PROGRESS.with(|progress| {
            *progress.borrow_mut() = None;
        });
    }
}

pub fn install_copy_progress(snapshot: CopyProgressSnapshot) -> CopyProgressGuard {
    CURRENT_COPY_PROGRESS.with(|progress| {
        *progress.borrow_mut() = Some(snapshot);
    });
    CopyProgressGuard
}

pub fn current_pg_stat_progress_copy_rows() -> Vec<Vec<Value>> {
    CURRENT_COPY_PROGRESS.with(|progress| {
        progress
            .borrow()
            .as_ref()
            .map(|snapshot| {
                vec![vec![
                    Value::Int32(snapshot.pid),
                    Value::Int64(i64::from(snapshot.datid)),
                    Value::Text(snapshot.datname.clone().into()),
                    Value::Int64(i64::from(snapshot.relid)),
                    Value::Text(snapshot.command.into()),
                    Value::Text(snapshot.copy_type.into()),
                    Value::Int64(snapshot.bytes_processed),
                    Value::Int64(snapshot.bytes_total),
                    Value::Int64(snapshot.tuples_processed),
                    Value::Int64(snapshot.tuples_excluded),
                    Value::Int64(snapshot.tuples_skipped),
                ]]
            })
            .unwrap_or_default()
    })
}
