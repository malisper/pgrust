// partdesc.c:35-46 PartitionDirectoryData / PartitionDirectoryEntry: the
// per-query directory that hands back the SAME PartitionDesc for a relation
// every time it is looked up while the directory lives, whatever concurrent
// DDL does to the relcache in between (planner: glob->partition_directory,
// pathnodes.h:181; executor: es_partition_directory, execnodes.h:687).
//
// Hosted here, below the executor, so EState can own one; the descriptor
// payload is type-erased because partdesc's PartitionDescData sits above the
// executor in the crate graph.  partdesc::PartitionDirectoryLookup owns the
// single downcast and is the only writer of entries.

use core::any::Any;
use std::rc::Rc;

use mcx::{Mcx, PgHashMap};
use types_core::Oid;

use crate::rel::Relation;

/// C PartitionDirectoryEntry (partdesc.c:41-46).
pub struct PartitionDirectoryEntry<'mcx> {
    /// C pde->rel: the relation pinned by RelationIncrementReferenceCount
    /// (partdesc.c:472) -- an alias's Rc strong count IS rd_refcnt -- so the
    /// descriptor it points at cannot be destroyed under us; released by
    /// DestroyPartitionDirectory (partdesc.c:485-492 = drop).
    pub rel: Relation<'mcx>,
    /// C pde->pd (partdesc's `Rc<PartitionDescData>`, erased).
    pub pd: Rc<dyn Any>,
}

/// C PartitionDirectoryData (partdesc.c:35-39); pdir_mcxt is the arena the
/// hash lives in.
pub struct PartitionDirectoryData<'mcx> {
    pub pdir_hash: PgHashMap<'mcx, Oid, PartitionDirectoryEntry<'mcx>>,
    pub omit_detached: bool,
}

impl<'mcx> PartitionDirectoryData<'mcx> {
    /// CreatePartitionDirectory (partdesc.c:423-445): an empty relid-keyed
    /// directory in `mcxt` carrying the omit_detached policy every lookup
    /// through it will use.
    pub fn new(mcxt: Mcx<'mcx>, omit_detached: bool) -> Self {
        PartitionDirectoryData {
            pdir_hash: PgHashMap::with_capacity_in(8, mcxt),
            omit_detached,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // audit-18.6 w2-057: the directory's two guarantees at the container
    // level (partdesc.c:447-492) -- an entry pins its relation for as long
    // as the directory lives (RelationIncrementReferenceCount ...
    // RelationDecrementReferenceCount) and hands the same descriptor back
    // on every lookup.
    #[test]
    fn entry_pins_relation_until_destroyed_and_keeps_descriptor_identity() {
        let cx = ::mcx::MemoryContext::new("partdir test");
        let mcx = cx.mcx();
        let rel = Relation::open(crate::rel::tests::rel_data(mcx, 16401), None);
        let pd: Rc<dyn Any> = Rc::new(7u32);
        let before = Rc::strong_count(rel.data_rc());

        let mut pdir = PartitionDirectoryData::new(mcx, true);
        assert!(pdir.omit_detached);
        pdir.pdir_hash.insert(
            rel.rd_id,
            PartitionDirectoryEntry { rel: rel.alias(), pd: Rc::clone(&pd) },
        );
        // partdesc.c:472: the entry holds one relation reference.
        assert_eq!(Rc::strong_count(rel.data_rc()), before + 1);
        // partdesc.c:447-453: same descriptor every time.
        let a = Rc::clone(&pdir.pdir_hash[&rel.rd_id].pd);
        let b = Rc::clone(&pdir.pdir_hash[&rel.rd_id].pd);
        assert!(Rc::ptr_eq(&a, &b) && Rc::ptr_eq(&a, &pd));
        drop((a, b));

        // partdesc.c:485-492: DestroyPartitionDirectory releases the pin.
        drop(pdir);
        assert_eq!(Rc::strong_count(rel.data_rc()), before);
        assert_eq!(Rc::strong_count(&pd), 1);
    }
}
