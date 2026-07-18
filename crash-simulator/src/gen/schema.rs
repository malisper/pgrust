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
    pub n_indexes: u32,
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
    next_table: u32,
    next_index: u32,
    next_rename: u32,
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
            if t.n_indexes > 0 {
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
            n_indexes: 0,
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
}
