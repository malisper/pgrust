//! Schema state tracked during generation, and the capabilities bitset
//! (capabilities gating, contract §2.1.2).
//!
//! Table identity: every table gets a stable BIRTH ID (`t<N>`) at CREATE.
//! `ALTER TABLE .. RENAME` changes only the current name; touched-table sets
//! (plan property blocks, shrinker dependency API) are recorded in birth-id
//! space, which makes rename-chasing automatic.

use rand::RngCore;

use crate::gen::profile::TableShape;
use crate::gen::weights::{range_incl, weighted_index};
use crate::property::Caps;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColType {
    Int,
    Bigint,
    Text,
    Numeric,
    Float8,
}

impl ColType {
    pub fn sql(self) -> &'static str {
        match self {
            ColType::Int => "int",
            ColType::Bigint => "bigint",
            ColType::Text => "text",
            ColType::Numeric => "numeric",
            ColType::Float8 => "float8",
        }
    }

    pub fn from_sql(s: &str) -> Option<ColType> {
        Some(match s {
            "int" => ColType::Int,
            "bigint" => ColType::Bigint,
            "text" => ColType::Text,
            "numeric" => ColType::Numeric,
            "float8" => ColType::Float8,
            _ => return None,
        })
    }

    /// Exact types are R7-safe in compared aggregate positions.
    pub fn is_exact(self) -> bool {
        !matches!(self, ColType::Float8)
    }
}

#[derive(Debug, Clone)]
pub struct Col {
    pub name: String,
    pub ty: ColType,
}

#[derive(Debug, Clone)]
pub struct Table {
    /// Stable birth id, e.g. "t3". Never changes.
    pub birth_id: String,
    /// Current SQL-visible name; changes on RENAME.
    pub cur_name: String,
    /// Payload columns (c1..cN). Implicit first column: `id bigint PRIMARY KEY`.
    pub cols: Vec<Col>,
    /// Monotonic insert-key counter (keys unique per table by construction).
    pub next_key: i64,
    /// Secondary indexes created on this table by the generator (H6: DROP
    /// INDEX needs a name; queries bias toward indexed columns; the implicit
    /// PK index is not listed).
    pub indexes: Vec<IndexDef>,
}

/// A generator-created secondary index.
#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    /// Plain single/multi-column key columns; empty for expression indexes.
    pub cols: Vec<String>,
}

/// A file_fdw foreign table the generator has set up (H6 state arm). The
/// backing CSV lives in the server's data directory under `csv_name`, written
/// by a `COPY ... TO` step; each engine leg writes its own copy of the same
/// deterministic bytes.
#[derive(Debug, Clone)]
pub struct ForeignTable {
    pub name: String,
    /// Payload columns after the leading `id bigint`.
    pub cols: Vec<Col>,
    /// Row count in the backing CSV.
    pub rows: u64,
    /// Absolute CSV path (`/tmp/simharness_fdw_<seed>_<name>.csv`). COPY TO
    /// requires an absolute path; the seed tag keeps concurrent campaigns
    /// from clobbering each other, and the seed is part of plan identity so
    /// the bytes stay a pure function of seed+profile+generator. Both legs
    /// write the same deterministic bytes to the same path — reads always
    /// see identical content on both legs.
    pub csv_name: String,
}

impl Table {
    pub fn cols_of_type(&self, pred: impl Fn(ColType) -> bool) -> Vec<&Col> {
        self.cols.iter().filter(|c| pred(c.ty)).collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SchemaState {
    /// Live tables only (drops remove).
    tables: Vec<Table>,
    /// Live file_fdw foreign tables (H6; separate from `tables` so ordinary
    /// DML/joins never address a read-only foreign table).
    foreign: Vec<ForeignTable>,
    /// file_fdw extension created this plan (transactional, snapshotted).
    fdw_extension: bool,
    /// file_fdw server created this plan (transactional, snapshotted).
    fdw_server: bool,
    next_table: u32,
    next_index: u32,
    next_rename: u32,
    next_foreign: u32,
    /// The plan's seed (set once at generator construction; not part of any
    /// snapshot). Used only to tag foreign-table CSV paths.
    plan_seed: u64,
}

/// Transaction-visible schema snapshot (see [`SchemaState::snapshot`]).
#[derive(Debug, Clone)]
pub struct SchemaSnapshot {
    tables: Vec<Table>,
    foreign: Vec<ForeignTable>,
    fdw_extension: bool,
    fdw_server: bool,
}

impl SchemaState {
    pub fn tables(&self) -> &[Table] {
        &self.tables
    }

    pub fn caps(&self) -> Caps {
        let mut c = Caps::default();
        if !self.tables.is_empty() {
            c = c.union(Caps::HAS_TABLE).union(Caps::HAS_UNIQUE_KEY);
        }
        if self.tables.len() > 1 {
            c = c.union(Caps::MULTIPLE_TABLES);
        }
        for t in &self.tables {
            if !t.indexes.is_empty() {
                c = c.union(Caps::HAS_INDEX);
            }
            for col in &t.cols {
                c = c.union(match col.ty {
                    ColType::Int | ColType::Bigint => Caps::HAS_INT_COL,
                    ColType::Text => Caps::HAS_TEXT_COL,
                    ColType::Float8 => Caps::HAS_FLOAT_COL,
                    ColType::Numeric => Caps::HAS_NUMERIC_COL,
                });
            }
        }
        c
    }

    pub fn pick_table(&self, rng: &mut dyn RngCore) -> Option<&Table> {
        if self.tables.is_empty() {
            return None;
        }
        let i = range_incl(rng, 0, self.tables.len() as u64 - 1) as usize;
        Some(&self.tables[i])
    }

    pub fn pick_table_idx(&self, rng: &mut dyn RngCore) -> Option<usize> {
        if self.tables.is_empty() {
            return None;
        }
        Some(range_incl(rng, 0, self.tables.len() as u64 - 1) as usize)
    }

    pub fn table_mut(&mut self, idx: usize) -> &mut Table {
        &mut self.tables[idx]
    }

    /// Create a new table shape from the profile distribution; returns its index.
    pub fn create_table(&mut self, rng: &mut dyn RngCore, shape: &TableShape) -> usize {
        self.next_table += 1;
        let birth = format!("t{}", self.next_table);
        let ncols = range_incl(rng, shape.min_cols as u64, shape.max_cols as u64) as usize;
        let tw = &shape.col_types;
        let type_weights = [tw.int, tw.bigint, tw.text, tw.numeric, tw.float8];
        let type_order =
            [ColType::Int, ColType::Bigint, ColType::Text, ColType::Numeric, ColType::Float8];
        let mut cols = Vec::with_capacity(ncols);
        for i in 1..=ncols {
            let ty = match weighted_index(rng, &type_weights) {
                Some(k) => type_order[k],
                None => ColType::Int, // all-zero col-type weights: degenerate but legal profile
            };
            cols.push(Col { name: format!("c{i}"), ty });
        }
        self.tables.push(Table {
            birth_id: birth.clone(),
            cur_name: birth,
            cols,
            next_key: 1,
            indexes: Vec::new(),
        });
        self.tables.len() - 1
    }

    pub fn next_index_name(&mut self) -> String {
        self.next_index += 1;
        format!("i{}", self.next_index)
    }

    /// Apply a rename to table `idx`; returns the new current name.
    pub fn rename_table(&mut self, idx: usize) -> String {
        self.next_rename += 1;
        let new_name = format!("{}_r{}", self.tables[idx].birth_id, self.next_rename);
        self.tables[idx].cur_name = new_name.clone();
        new_name
    }

    /// Drop table `idx` (caller guards len() > 1 so capabilities never regress
    /// to empty).
    pub fn drop_table(&mut self, idx: usize) -> Table {
        self.tables.remove(idx)
    }

    /// Transaction-visible schema snapshot (transactional-DDL modeling: DDL in
    /// PostgreSQL is transactional, so ROLLBACK / ROLLBACK TO SAVEPOINT / an
    /// aborting disconnect reverts it on the server; the generator's model
    /// must revert with it or every later statement addresses tables the
    /// server rolled away — 42P01/25P02 storms).
    ///
    /// The global name counters (`next_table` / `next_index` / `next_rename`
    /// / `next_foreign`) are deliberately NOT part of the snapshot:
    /// identifiers stay monotonic across rollbacks, so a name is never
    /// reused. A rolled-back name is free on the server (skipping it is
    /// always valid SQL), and birth ids stay unique plan-wide, which the
    /// table-dependency API relies on.
    ///
    /// H6: the snapshot also covers the fdw state — CREATE EXTENSION /
    /// CREATE SERVER / CREATE FOREIGN TABLE are all transactional in
    /// PostgreSQL, so they revert with a ROLLBACK. (The CSV written by the
    /// `COPY ... TO` step does NOT revert — a data-directory file with no
    /// foreign table over it is harmless, and re-setup uses IF NOT EXISTS.)
    pub fn snapshot(&self) -> SchemaSnapshot {
        SchemaSnapshot {
            tables: self.tables.clone(),
            foreign: self.foreign.clone(),
            fdw_extension: self.fdw_extension,
            fdw_server: self.fdw_server,
        }
    }

    /// Restore the tx-visible state captured by [`snapshot`].
    pub fn restore(&mut self, snap: SchemaSnapshot) {
        self.tables = snap.tables;
        self.foreign = snap.foreign;
        self.fdw_extension = snap.fdw_extension;
        self.fdw_server = snap.fdw_server;
    }

    // -- H6 foreign-table state ops (file_fdw) ------------------------------

    pub fn foreign_tables(&self) -> &[ForeignTable] {
        &self.foreign
    }

    pub fn fdw_extension_created(&self) -> bool {
        self.fdw_extension
    }

    pub fn fdw_server_created(&self) -> bool {
        self.fdw_server
    }

    pub fn mark_fdw_extension(&mut self) {
        self.fdw_extension = true;
    }

    pub fn mark_fdw_server(&mut self) {
        self.fdw_server = true;
    }

    pub fn set_plan_seed(&mut self, seed: u64) {
        self.plan_seed = seed;
    }

    /// Register a new foreign table shape (fixed simple payload: one int, one
    /// text column — deterministic, matched by the CSV writer in gen::noise).
    pub fn create_foreign_table(&mut self, rows: u64) -> &ForeignTable {
        self.next_foreign += 1;
        let name = format!("ft{}", self.next_foreign);
        let csv_name = format!("/tmp/simharness_fdw_{}_{name}.csv", self.plan_seed);
        self.foreign.push(ForeignTable {
            name,
            cols: vec![
                Col { name: "a".into(), ty: ColType::Int },
                Col { name: "b".into(), ty: ColType::Text },
            ],
            rows,
            csv_name,
        });
        self.foreign.last().expect("just pushed")
    }

    pub fn pick_foreign(&self, rng: &mut dyn RngCore) -> Option<&ForeignTable> {
        if self.foreign.is_empty() {
            return None;
        }
        let i = range_incl(rng, 0, self.foreign.len() as u64 - 1) as usize;
        Some(&self.foreign[i])
    }
}
