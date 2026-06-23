//! Partition-key cache vocabulary (`utils/partcache.h`,
//! `nodes/parsenodes.h` partition-strategy constants), trimmed to the items
//! the `backend-utils-cache-partcache` port consumes.
//!
//! `PartitionKeyData` is the relcache-cached partition key built by
//! `RelationBuildPartitionKey`; its per-attribute arrays are palloc'd into the
//! relation's `rd_partkeycxt` (a child of `CacheMemoryContext`), so the owned
//! model carries them as `mcx`-allocated `PgVec`s.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use mcx::{slice_in, vec_with_capacity_in, Mcx, PgVec};
use ::types_core::fmgr::FmgrInfo;
use ::types_core::primitive::{AttrNumber, Oid};
use ::types_error::PgResult;

/// `PartitionStrategy` (`nodes/parsenodes.h`) — the partitioning strategy,
/// a `char`-valued code stored in `pg_partitioned_table.partstrat`.
pub type PartitionStrategy = i8;

/// `PARTITION_STRATEGY_LIST` (`'l'`).
pub const PARTITION_STRATEGY_LIST: PartitionStrategy = b'l' as PartitionStrategy;
/// `PARTITION_STRATEGY_RANGE` (`'r'`).
pub const PARTITION_STRATEGY_RANGE: PartitionStrategy = b'r' as PartitionStrategy;
/// `PARTITION_STRATEGY_HASH` (`'h'`).
pub const PARTITION_STRATEGY_HASH: PartitionStrategy = b'h' as PartitionStrategy;

/// `BTORDER_PROC` (`access/nbtree.h`) — the btree "order" support function
/// number (amproc 1), used for LIST/RANGE partition keys.
pub const BTORDER_PROC: i16 = 1;

/// `PartitionKeyData` (`utils/partcache.h`) — information about the partition
/// key of a relation, built by `RelationBuildPartitionKey` and cached on the
/// relcache entry (`rd_partkey`). The per-attribute arrays are allocated in
/// the relation's own `rd_partkeycxt`; the owned model carries them as
/// `mcx`-allocated vectors of length `partnatts`.
#[derive(Debug)]
pub struct PartitionKeyData<'mcx> {
    /// `strategy` — partitioning strategy.
    pub strategy: PartitionStrategy,
    /// `partnatts` — number of columns in the partition key.
    pub partnatts: i16,
    /// `partattrs` — attribute numbers of the key columns, or 0 for an
    /// expression column.
    pub partattrs: PgVec<'mcx, AttrNumber>,
    /// `partexprs` — list of expressions in the partitioning key, one for
    /// each zero-valued `partattrs`.
    pub partexprs: PgVec<'mcx, nodes::Expr<'mcx>>,
    /// `partopfamily` — OIDs of operator families.
    pub partopfamily: PgVec<'mcx, Oid>,
    /// `partopcintype` — OIDs of opclass declared input data types.
    pub partopcintype: PgVec<'mcx, Oid>,
    /// `partsupfunc` — lookup info for support funcs.
    pub partsupfunc: PgVec<'mcx, FmgrInfo>,
    /// `partcollation` — partitioning collation per attribute.
    pub partcollation: PgVec<'mcx, Oid>,
    /// `parttypid` — type OID per attribute.
    pub parttypid: PgVec<'mcx, Oid>,
    /// `parttypmod` — type modifier per attribute.
    pub parttypmod: PgVec<'mcx, i32>,
    /// `parttyplen` — type length per attribute.
    pub parttyplen: PgVec<'mcx, i16>,
    /// `parttypbyval` — pass-by-value flag per attribute.
    pub parttypbyval: PgVec<'mcx, bool>,
    /// `parttypalign` — alignment code per attribute.
    pub parttypalign: PgVec<'mcx, i8>,
    /// `parttypcoll` — type's collation per attribute.
    pub parttypcoll: PgVec<'mcx, Oid>,
}

/// The `pg_partitioned_table` tuple `RelationBuildPartitionKey` reads via
/// `SearchSysCache1(PARTRELID, ...)` + `GETSTRUCT` + `SysCacheGetAttr*`, with
/// the `partexprs` `pg_node_tree` text already de-stringized
/// (`stringToNode`), const-simplified (`eval_const_expressions`), and
/// opfuncid-fixed (`fix_opfuncids`), then `copyObject`. The
/// `int2vector`/`oidvector` columns are decoded into their value slices and
/// the whole tuple is returned by value (so there is no separate
/// `ReleaseSysCache`); each vector is allocated in the caller's `mcx`.
#[derive(Debug)]
pub struct PartrelTupleData<'mcx> {
    /// `form->partstrat`.
    pub strategy: PartitionStrategy,
    /// `form->partnatts`.
    pub partnatts: i16,
    /// `form->partattrs.values` — the `int2vector` of key attribute numbers
    /// (0 marks an expression column).
    pub partattrs: PgVec<'mcx, AttrNumber>,
    /// `partclass` oidvector values — the per-key operator-class OIDs.
    pub partclass: PgVec<'mcx, Oid>,
    /// `partcollation` oidvector values — the per-key collation OIDs.
    pub partcollation: PgVec<'mcx, Oid>,
    /// the processed `partexprs` list (NIL ⇒ empty).
    pub partexprs: PgVec<'mcx, nodes::Expr<'mcx>>,
}

/// Per-partition-key opclass facts resolved from one `pg_opclass` tuple
/// (`SearchSysCache1(CLAOID, partclass[i])` + `GETSTRUCT` +
/// `get_opfamily_proc`).
#[derive(Debug)]
pub struct PartKeyOpInfo<'mcx> {
    /// `opclassform->opcfamily`.
    pub opcfamily: Oid,
    /// `opclassform->opcintype`.
    pub opcintype: Oid,
    /// support-function OID from `get_opfamily_proc(opcfamily, opcintype,
    /// opcintype, procnum)`; `InvalidOid` if none (the caller raises the
    /// missing-support-function error).
    pub support_funcid: Oid,
    /// `NameStr(opclassform->opcname)` — used only to build the
    /// missing-support-function error message.
    pub opcname: ::mcx::PgString<'mcx>,
}

/// Per-key type information: the result of the `attno != 0`
/// (`TupleDescAttr(rel->rd_att, attno - 1)`) branch or the expression branch
/// (`exprType`/`exprTypmod`/`exprCollation` of the matching `partexprs` cell),
/// followed by `get_typlenbyvalalign`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PartKeyTypeInfo {
    /// `att->atttypid` or `exprType(...)`.
    pub typid: Oid,
    /// `att->atttypmod` or `exprTypmod(...)`.
    pub typmod: i32,
    /// `att->attcollation` or `exprCollation(...)`.
    pub typcoll: Oid,
    /// `get_typlenbyvalalign` length.
    pub typlen: i16,
    /// `get_typlenbyvalalign` by-value flag.
    pub typbyval: bool,
    /// `get_typlenbyvalalign` align code.
    pub typalign: i8,
}

impl<'mcx> PartitionKeyData<'mcx> {
    /// Deep copy into `mcx` (C: the relcache caches the key under
    /// `rd_partkeycxt`; the owned model copies the per-attribute vectors).
    /// Fallible: copying allocates. `partexprs` clones the (`Clone`) `Expr`
    /// cells element-wise into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PartitionKeyData<'b>> {
        Ok(PartitionKeyData {
            strategy: self.strategy,
            partnatts: self.partnatts,
            partattrs: slice_in(mcx, &self.partattrs)?,
            partexprs: {
                // Deep-clone each Expr into the destination arena `'b` (slice_in
                // would do a shallow copy that cannot retie the invariant 'mcx).
                let mut v = vec_with_capacity_in(mcx, self.partexprs.len())?;
                for e in self.partexprs.iter() {
                    v.push(e.clone_in(mcx)?);
                }
                v
            },
            partopfamily: slice_in(mcx, &self.partopfamily)?,
            partopcintype: slice_in(mcx, &self.partopcintype)?,
            partsupfunc: slice_in(mcx, &self.partsupfunc)?,
            partcollation: slice_in(mcx, &self.partcollation)?,
            parttypid: slice_in(mcx, &self.parttypid)?,
            parttypmod: slice_in(mcx, &self.parttypmod)?,
            parttyplen: slice_in(mcx, &self.parttyplen)?,
            parttypbyval: slice_in(mcx, &self.parttypbyval)?,
            parttypalign: slice_in(mcx, &self.parttypalign)?,
            parttypcoll: slice_in(mcx, &self.parttypcoll)?,
        })
    }

    /// `get_partition_strategy(key)` (`utils/partcache.h`): `key->strategy`.
    pub fn get_partition_strategy(&self) -> i32 {
        self.strategy as i32
    }

    /// `get_partition_natts(key)` (`utils/partcache.h`): `key->partnatts`.
    pub fn get_partition_natts(&self) -> i32 {
        self.partnatts as i32
    }

    /// `get_partition_exprs(key)` (`utils/partcache.h`): `key->partexprs`.
    pub fn get_partition_exprs(&self) -> &[nodes::Expr<'mcx>] {
        &self.partexprs
    }

    /// `get_partition_col_attnum(key, col)` (`utils/partcache.h`).
    pub fn get_partition_col_attnum(&self, col: usize) -> i16 {
        self.partattrs[col]
    }

    /// `get_partition_col_typid(key, col)` (`utils/partcache.h`).
    pub fn get_partition_col_typid(&self, col: usize) -> Oid {
        self.parttypid[col]
    }

    /// `get_partition_col_typmod(key, col)` (`utils/partcache.h`).
    pub fn get_partition_col_typmod(&self, col: usize) -> i32 {
        self.parttypmod[col]
    }

    /// `get_partition_col_collation(key, col)` (`utils/partcache.h`).
    pub fn get_partition_col_collation(&self, col: usize) -> Oid {
        self.partcollation[col]
    }
}
