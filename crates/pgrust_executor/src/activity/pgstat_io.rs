use pgrust_nodes::datetime::TimestampTzADT;

use super::pgstat::{SessionStatsState, now_timestamptz};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IoStatsKey {
    pub backend_type: String,
    pub object: String,
    pub context: String,
}

impl IoStatsKey {
    pub fn new(
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
pub struct IoStatsEntry {
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
    pub fn apply_delta(&mut self, delta: &IoStatsDelta) {
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
pub struct IoStatsDelta {
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
    fn note_io_delta(
        &mut self,
        backend_type: &str,
        object: &str,
        context: &str,
        update: impl Fn(&mut IoStatsDelta, &mut IoStatsEntry),
    ) {
        let key = IoStatsKey::new(backend_type, object, context);
        let delta = self.pending_flush.io.entry(key.clone()).or_default();
        let backend_entry = self.backend_io.entry(key).or_default();
        update(delta, backend_entry);
        delta.touched = true;
        if backend_entry.stats_reset.is_none() {
            backend_entry.stats_reset = Some(now_timestamptz());
        }
        self.clear_snapshot();
    }

    pub fn note_io_read(&mut self, backend_type: &str, object: &str, context: &str, bytes: i64) {
        self.note_io_delta(backend_type, object, context, |delta, backend| {
            delta.reads += 1;
            delta.read_bytes += bytes;
            backend.reads += 1;
            backend.read_bytes += bytes;
        });
    }

    pub fn note_io_hit(&mut self, backend_type: &str, object: &str, context: &str) {
        self.note_io_delta(backend_type, object, context, |delta, backend| {
            delta.hits += 1;
            backend.hits += 1;
        });
    }

    pub fn note_io_write(&mut self, backend_type: &str, object: &str, context: &str, bytes: i64) {
        self.note_io_delta(backend_type, object, context, |delta, backend| {
            delta.writes += 1;
            delta.write_bytes += bytes;
            backend.writes += 1;
            backend.write_bytes += bytes;
        });
    }

    pub fn note_io_extend(&mut self, backend_type: &str, object: &str, context: &str, bytes: i64) {
        self.note_io_delta(backend_type, object, context, |delta, backend| {
            delta.extends += 1;
            delta.extend_bytes += bytes;
            backend.extends += 1;
            backend.extend_bytes += bytes;
        });
    }

    pub fn note_io_fsync(&mut self, backend_type: &str, object: &str, context: &str) {
        self.note_io_delta(backend_type, object, context, |delta, backend| {
            delta.fsyncs += 1;
            backend.fsyncs += 1;
        });
    }

    pub fn note_io_eviction(&mut self, backend_type: &str, object: &str, context: &str) {
        self.note_io_delta(backend_type, object, context, |delta, backend| {
            delta.evictions += 1;
            backend.evictions += 1;
        });
    }

    pub fn note_io_reuse(&mut self, backend_type: &str, object: &str, context: &str) {
        self.note_io_delta(backend_type, object, context, |delta, backend| {
            delta.reuses += 1;
            backend.reuses += 1;
        });
    }
}

pub fn default_pg_stat_io_keys() -> Vec<IoStatsKey> {
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
