use crate::include::nodes::datetime::TimestampTzADT;

use super::pgstat::{SessionStatsState, now_timestamptz};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct IoStatsKey {
    pub backend_type: String,
    pub object: String,
    pub context: String,
}

impl IoStatsKey {
    pub(crate) fn new(
        backend_type: impl Into<String>,
        object: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        Self {
            backend_type: backend_type.into(),
            object: object.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct IoStatsEntry {
    pub reads: i64,
    pub read_bytes: i64,
    pub read_time_micros: u64,
    pub writes: i64,
    pub write_bytes: i64,
    pub write_time_micros: u64,
    pub writebacks: i64,
    pub writeback_time_micros: u64,
    pub extends: i64,
    pub extend_bytes: i64,
    pub extend_time_micros: u64,
    pub hits: i64,
    pub evictions: i64,
    pub reuses: i64,
    pub fsyncs: i64,
    pub fsync_time_micros: u64,
    pub stats_reset: Option<TimestampTzADT>,
}

impl IoStatsEntry {
    pub(crate) fn apply_delta(&mut self, delta: &IoStatsDelta) {
        self.reads += delta.reads;
        self.read_bytes += delta.read_bytes;
        self.read_time_micros += delta.read_time_micros;
        self.writes += delta.writes;
        self.write_bytes += delta.write_bytes;
        self.write_time_micros += delta.write_time_micros;
        self.writebacks += delta.writebacks;
        self.writeback_time_micros += delta.writeback_time_micros;
        self.extends += delta.extends;
        self.extend_bytes += delta.extend_bytes;
        self.extend_time_micros += delta.extend_time_micros;
        self.hits += delta.hits;
        self.evictions += delta.evictions;
        self.reuses += delta.reuses;
        self.fsyncs += delta.fsyncs;
        self.fsync_time_micros += delta.fsync_time_micros;
        if delta.touched && self.stats_reset.is_none() {
            self.stats_reset = Some(now_timestamptz());
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct IoStatsDelta {
    pub reads: i64,
    pub read_bytes: i64,
    pub read_time_micros: u64,
    pub writes: i64,
    pub write_bytes: i64,
    pub write_time_micros: u64,
    pub writebacks: i64,
    pub writeback_time_micros: u64,
    pub extends: i64,
    pub extend_bytes: i64,
    pub extend_time_micros: u64,
    pub hits: i64,
    pub evictions: i64,
    pub reuses: i64,
    pub fsyncs: i64,
    pub fsync_time_micros: u64,
    pub touched: bool,
}

impl SessionStatsState {
    pub(crate) fn note_io_read(
        &mut self,
        backend_type: &str,
        object: &str,
        context: &str,
        bytes: i64,
    ) {
        let delta = self
            .pending_flush
            .io
            .entry(IoStatsKey::new(backend_type, object, context))
            .or_default();
        delta.reads += 1;
        delta.read_bytes += bytes;
        delta.touched = true;
        self.clear_snapshot();
    }

    pub(crate) fn note_io_hit(&mut self, backend_type: &str, object: &str, context: &str) {
        let delta = self
            .pending_flush
            .io
            .entry(IoStatsKey::new(backend_type, object, context))
            .or_default();
        delta.hits += 1;
        delta.touched = true;
        self.clear_snapshot();
    }

    pub(crate) fn note_io_write(
        &mut self,
        backend_type: &str,
        object: &str,
        context: &str,
        bytes: i64,
    ) {
        let delta = self
            .pending_flush
            .io
            .entry(IoStatsKey::new(backend_type, object, context))
            .or_default();
        delta.writes += 1;
        delta.write_bytes += bytes;
        delta.touched = true;
        self.clear_snapshot();
    }
}

pub(crate) fn default_pg_stat_io_keys() -> Vec<IoStatsKey> {
    let rows = [
        ("autovacuum launcher", "relation", "bulkread"),
        ("autovacuum launcher", "relation", "init"),
        ("autovacuum launcher", "relation", "normal"),
        ("autovacuum launcher", "wal", "init"),
        ("autovacuum launcher", "wal", "normal"),
        ("autovacuum worker", "relation", "bulkread"),
        ("autovacuum worker", "relation", "init"),
        ("autovacuum worker", "relation", "normal"),
        ("autovacuum worker", "relation", "vacuum"),
        ("autovacuum worker", "wal", "init"),
        ("autovacuum worker", "wal", "normal"),
        ("background worker", "relation", "bulkread"),
        ("background worker", "relation", "bulkwrite"),
        ("background worker", "relation", "init"),
        ("background worker", "relation", "normal"),
        ("background worker", "relation", "vacuum"),
        ("background worker", "temp relation", "normal"),
        ("background worker", "wal", "init"),
        ("background worker", "wal", "normal"),
        ("background writer", "relation", "init"),
        ("background writer", "relation", "normal"),
        ("background writer", "wal", "init"),
        ("background writer", "wal", "normal"),
        ("checkpointer", "relation", "init"),
        ("checkpointer", "relation", "normal"),
        ("checkpointer", "wal", "init"),
        ("checkpointer", "wal", "normal"),
        ("client backend", "relation", "bulkread"),
        ("client backend", "relation", "bulkwrite"),
        ("client backend", "relation", "init"),
        ("client backend", "relation", "normal"),
        ("client backend", "relation", "vacuum"),
        ("client backend", "temp relation", "normal"),
        ("client backend", "wal", "init"),
        ("client backend", "wal", "normal"),
        ("io worker", "relation", "bulkread"),
        ("io worker", "relation", "bulkwrite"),
        ("io worker", "relation", "init"),
        ("io worker", "relation", "normal"),
        ("io worker", "relation", "vacuum"),
        ("io worker", "temp relation", "normal"),
        ("io worker", "wal", "init"),
        ("io worker", "wal", "normal"),
        ("slotsync worker", "relation", "bulkread"),
        ("slotsync worker", "relation", "bulkwrite"),
        ("slotsync worker", "relation", "init"),
        ("slotsync worker", "relation", "normal"),
        ("slotsync worker", "relation", "vacuum"),
        ("slotsync worker", "temp relation", "normal"),
        ("slotsync worker", "wal", "init"),
        ("slotsync worker", "wal", "normal"),
        ("standalone backend", "relation", "bulkread"),
        ("standalone backend", "relation", "bulkwrite"),
        ("standalone backend", "relation", "init"),
        ("standalone backend", "relation", "normal"),
        ("standalone backend", "relation", "vacuum"),
        ("standalone backend", "wal", "init"),
        ("standalone backend", "wal", "normal"),
        ("startup", "relation", "bulkread"),
        ("startup", "relation", "bulkwrite"),
        ("startup", "relation", "init"),
        ("startup", "relation", "normal"),
        ("startup", "relation", "vacuum"),
        ("startup", "wal", "init"),
        ("startup", "wal", "normal"),
        ("walreceiver", "wal", "init"),
        ("walreceiver", "wal", "normal"),
        ("walsender", "relation", "bulkread"),
        ("walsender", "relation", "bulkwrite"),
        ("walsender", "relation", "init"),
        ("walsender", "relation", "normal"),
        ("walsender", "relation", "vacuum"),
        ("walsender", "temp relation", "normal"),
        ("walsender", "wal", "init"),
        ("walsender", "wal", "normal"),
        ("walsummarizer", "wal", "init"),
        ("walsummarizer", "wal", "normal"),
        ("walwriter", "wal", "init"),
        ("walwriter", "wal", "normal"),
    ];
    rows.into_iter()
        .map(|(backend_type, object, context)| IoStatsKey::new(backend_type, object, context))
        .collect()
}
