//! SP-GiST opclass support-function argument structs from
//! `src/include/access/spgist.h`.
//!
//! These are the ABI struct layouts passed (by pointer, via the fmgr calling
//! convention) to an SP-GiST opclass's `config`/`choose`/`picksplit`/
//! `inner_consistent`/`leaf_consistent` support procedures.  They are expressed
//! here as `#[repr(C)]` exact-layout structs with compile-time size/offset
//! assertions so the safe Rust opclass code (e.g.
//! [`backend_access_spgist_geoproc`](../../backend-access-spgist-geoproc)) shares
//! one ABI definition with the rest of the tree.

use core::ffi::{c_int, c_void};

use crate::memory::MemoryContext;
use crate::scankey::ScanKey;
use crate::{Datum, Oid};

/// `SPGIST_CONFIG_PROC` (spgist.h:23).
pub const SPGIST_CONFIG_PROC: u16 = 1;
/// `SPGIST_CHOOSE_PROC` (spgist.h:24).
pub const SPGIST_CHOOSE_PROC: u16 = 2;
/// `SPGIST_PICKSPLIT_PROC` (spgist.h:25).
pub const SPGIST_PICKSPLIT_PROC: u16 = 3;
/// `SPGIST_INNER_CONSISTENT_PROC` (spgist.h:26).
pub const SPGIST_INNER_CONSISTENT_PROC: u16 = 4;
/// `SPGIST_LEAF_CONSISTENT_PROC` (spgist.h:27).
pub const SPGIST_LEAF_CONSISTENT_PROC: u16 = 5;
/// `SPGIST_COMPRESS_PROC` (spgist.h:28).
pub const SPGIST_COMPRESS_PROC: u16 = 6;
/// `SPGIST_OPTIONS_PROC` (spgist.h:29).
pub const SPGIST_OPTIONS_PROC: u16 = 7;
/// `SPGISTNRequiredProc` (spgist.h:30).
pub const SPGISTNRequiredProc: u16 = 5;
/// `SPGISTNProc` (spgist.h:31).
pub const SPGISTNProc: u16 = 7;

/// `spgConfigIn` (spgist.h:36) -- input to the `config` support proc.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgConfigIn {
    /// Data type to be indexed.
    pub attType: Oid,
}

/// `spgConfigOut` (spgist.h:42) -- output of the `config` support proc.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgConfigOut {
    /// Data type of inner-tuple prefixes.
    pub prefixType: Oid,
    /// Data type of inner-tuple node labels.
    pub labelType: Oid,
    /// Data type of leaf-tuple values.
    pub leafType: Oid,
    /// Opclass can reconstruct original data.
    pub canReturnData: bool,
    /// Opclass can cope with values > 1 page.
    pub longValuesOK: bool,
}

/// `spgChooseIn` (spgist.h:53) -- input to the `choose` support proc.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgChooseIn {
    /// Original datum to be indexed.
    pub datum: Datum,
    /// Current datum to be stored at leaf.
    pub leafDatum: Datum,
    /// Current level (counting from zero).
    pub level: c_int,
    /// Tuple is marked all-the-same?
    pub allTheSame: bool,
    /// Tuple has a prefix?
    pub hasPrefix: bool,
    /// If so, the prefix value.
    pub prefixDatum: Datum,
    /// Number of nodes in the inner tuple.
    pub nNodes: c_int,
    /// Node label values (NULL if none).
    pub nodeLabels: *mut Datum,
}

/// `spgChooseResultType` (spgist.h:66): descend into existing node.
pub const spgMatchNode: u32 = 1;
/// `spgChooseResultType` (spgist.h:66): add a node to the inner tuple.
pub const spgAddNode: u32 = 2;
/// `spgChooseResultType` (spgist.h:66): split inner tuple (change prefix).
pub const spgSplitTuple: u32 = 3;
/// `spgChooseResultType` (spgist.h:66).
pub type spgChooseResultType = u32;

/// `spgChooseOut.result.matchNode` (spgist.h:79).
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgChooseOutMatchNode {
    /// Descend to this node (index from 0).
    pub nodeN: c_int,
    /// Increment level by this much.
    pub levelAdd: c_int,
    /// New leaf datum.
    pub restDatum: Datum,
}

/// `spgChooseOut.result.addNode` (spgist.h:85).
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgChooseOutAddNode {
    /// New node's label.
    pub nodeLabel: Datum,
    /// Where to insert it (index from 0).
    pub nodeN: c_int,
}

/// `spgChooseOut.result.splitTuple` (spgist.h:90).
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgChooseOutSplitTuple {
    /// Upper tuple should have a prefix?
    pub prefixHasPrefix: bool,
    /// If so, its value.
    pub prefixPrefixDatum: Datum,
    /// Number of nodes.
    pub prefixNNodes: c_int,
    /// Their labels (or NULL for no labels).
    pub prefixNodeLabels: *mut Datum,
    /// Which node gets child tuple.
    pub childNodeN: c_int,
    /// Lower tuple should have a prefix?
    pub postfixHasPrefix: bool,
    /// If so, its value.
    pub postfixPrefixDatum: Datum,
}

/// `spgChooseOut.result` union (spgist.h:76).
#[derive(Clone, Copy)]
#[repr(C)]
pub union spgChooseOutResult {
    /// Results for `spgMatchNode`.
    pub matchNode: spgChooseOutMatchNode,
    /// Results for `spgAddNode`.
    pub addNode: spgChooseOutAddNode,
    /// Results for `spgSplitTuple`.
    pub splitTuple: spgChooseOutSplitTuple,
}

/// `spgChooseOut` (spgist.h:73) -- output of the `choose` support proc.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct spgChooseOut {
    /// Action code (`spgMatchNode`/`spgAddNode`/`spgSplitTuple`).
    pub resultType: spgChooseResultType,
    /// Result union, selected by `resultType`.
    pub result: spgChooseOutResult,
}

/// `spgPickSplitIn` (spgist.h:111) -- input to the `picksplit` support proc.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgPickSplitIn {
    /// Number of leaf tuples.
    pub nTuples: c_int,
    /// Their datums (array of length `nTuples`).
    pub datums: *mut Datum,
    /// Current level (counting from zero).
    pub level: c_int,
}

/// `spgPickSplitOut` (spgist.h:118) -- output of the `picksplit` support proc.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgPickSplitOut {
    /// New inner tuple should have a prefix?
    pub hasPrefix: bool,
    /// If so, its value.
    pub prefixDatum: Datum,
    /// Number of nodes for new inner tuple.
    pub nNodes: c_int,
    /// Their labels (or NULL for no labels).
    pub nodeLabels: *mut Datum,
    /// Node index for each leaf tuple.
    pub mapTuplesToNodes: *mut c_int,
    /// Datum to store in each new leaf tuple.
    pub leafTupleDatums: *mut Datum,
}

/// `spgInnerConsistentIn` (spgist.h:130) -- input to the `inner_consistent`
/// support proc.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgInnerConsistentIn {
    /// Array of operators and comparison values.
    pub scankeys: ScanKey,
    /// Array of ordering operators and comparison values.
    pub orderbys: ScanKey,
    /// Length of `scankeys` array.
    pub nkeys: c_int,
    /// Length of `orderbys` array.
    pub norderbys: c_int,
    /// Value reconstructed at parent.
    pub reconstructedValue: Datum,
    /// Opclass-specific traverse value.
    pub traversalValue: *mut c_void,
    /// Put new traverse values here.
    pub traversalMemoryContext: MemoryContext,
    /// Current level (counting from zero).
    pub level: c_int,
    /// Original data must be returned?
    pub returnData: bool,
    /// Tuple is marked all-the-same?
    pub allTheSame: bool,
    /// Tuple has a prefix?
    pub hasPrefix: bool,
    /// If so, the prefix value.
    pub prefixDatum: Datum,
    /// Number of nodes in the inner tuple.
    pub nNodes: c_int,
    /// Node label values (NULL if none).
    pub nodeLabels: *mut Datum,
}

/// `spgInnerConsistentOut` (spgist.h:149) -- output of the `inner_consistent`
/// support proc.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgInnerConsistentOut {
    /// Number of child nodes to be visited.
    pub nNodes: c_int,
    /// Their indexes in the node array.
    pub nodeNumbers: *mut c_int,
    /// Increment level by this much for each.
    pub levelAdds: *mut c_int,
    /// Associated reconstructed values.
    pub reconstructedValues: *mut Datum,
    /// Opclass-specific traverse values.
    pub traversalValues: *mut *mut c_void,
    /// Associated distances.
    pub distances: *mut *mut f64,
}

/// `spgLeafConsistentIn` (spgist.h:161) -- input to the `leaf_consistent`
/// support proc.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgLeafConsistentIn {
    /// Array of operators and comparison values.
    pub scankeys: ScanKey,
    /// Array of ordering operators and comparison values.
    pub orderbys: ScanKey,
    /// Length of `scankeys` array.
    pub nkeys: c_int,
    /// Length of `orderbys` array.
    pub norderbys: c_int,
    /// Value reconstructed at parent.
    pub reconstructedValue: Datum,
    /// Opclass-specific traverse value.
    pub traversalValue: *mut c_void,
    /// Current level (counting from zero).
    pub level: c_int,
    /// Original data must be returned?
    pub returnData: bool,
    /// Datum in leaf tuple.
    pub leafDatum: Datum,
}

/// `spgLeafConsistentOut` (spgist.h:174) -- output of the `leaf_consistent`
/// support proc.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct spgLeafConsistentOut {
    /// Reconstructed original data, if any.
    pub leafValue: Datum,
    /// Set true if operator must be rechecked.
    pub recheck: bool,
    /// Set true if distances must be rechecked.
    pub recheckDistances: bool,
    /// Associated distances.
    pub distances: *mut f64,
}

#[cfg(test)]
mod abi_tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    // 64-bit layout (LP64): pointers/Datum/MemoryContext are 8 bytes, Oid/c_int
    // 4 bytes.  Matches the C compiler's layout of spgist.h on a 64-bit build.

    #[test]
    fn config_layout() {
        assert_eq!(size_of::<spgConfigIn>(), 4);
        assert_eq!(offset_of!(spgConfigOut, prefixType), 0);
        assert_eq!(offset_of!(spgConfigOut, labelType), 4);
        assert_eq!(offset_of!(spgConfigOut, leafType), 8);
        assert_eq!(offset_of!(spgConfigOut, canReturnData), 12);
        assert_eq!(offset_of!(spgConfigOut, longValuesOK), 13);
        assert_eq!(size_of::<spgConfigOut>(), 16);
    }

    #[test]
    fn choose_in_layout() {
        assert_eq!(offset_of!(spgChooseIn, datum), 0);
        assert_eq!(offset_of!(spgChooseIn, leafDatum), 8);
        assert_eq!(offset_of!(spgChooseIn, level), 16);
        assert_eq!(offset_of!(spgChooseIn, allTheSame), 20);
        assert_eq!(offset_of!(spgChooseIn, hasPrefix), 21);
        assert_eq!(offset_of!(spgChooseIn, prefixDatum), 24);
        assert_eq!(offset_of!(spgChooseIn, nNodes), 32);
        assert_eq!(offset_of!(spgChooseIn, nodeLabels), 40);
        assert_eq!(size_of::<spgChooseIn>(), 48);
    }

    #[test]
    fn choose_out_layout() {
        // The widest arm is `splitTuple`:
        //   bool(1)+pad(7)+Datum(8)+int(4)+pad(4)+ptr(8)+int(4)+bool(1)+pad(3)
        //   +Datum(8) = 48 bytes, 8-aligned.  So the union is 48 bytes, and the
        // struct is resultType(int,4)+pad(4)+union(48) = 56 bytes.
        assert_eq!(align_of::<spgChooseOutResult>(), 8);
        assert_eq!(size_of::<spgChooseOutResult>(), 48);
        assert_eq!(offset_of!(spgChooseOut, resultType), 0);
        assert_eq!(offset_of!(spgChooseOut, result), 8);
        assert_eq!(size_of::<spgChooseOut>(), 56);
        // matchNode arm
        assert_eq!(offset_of!(spgChooseOutMatchNode, nodeN), 0);
        assert_eq!(offset_of!(spgChooseOutMatchNode, levelAdd), 4);
        assert_eq!(offset_of!(spgChooseOutMatchNode, restDatum), 8);
        // splitTuple arm
        assert_eq!(offset_of!(spgChooseOutSplitTuple, prefixHasPrefix), 0);
        assert_eq!(offset_of!(spgChooseOutSplitTuple, prefixPrefixDatum), 8);
        assert_eq!(offset_of!(spgChooseOutSplitTuple, prefixNNodes), 16);
        assert_eq!(offset_of!(spgChooseOutSplitTuple, prefixNodeLabels), 24);
        assert_eq!(offset_of!(spgChooseOutSplitTuple, childNodeN), 32);
        assert_eq!(offset_of!(spgChooseOutSplitTuple, postfixHasPrefix), 36);
        assert_eq!(offset_of!(spgChooseOutSplitTuple, postfixPrefixDatum), 40);
    }

    #[test]
    fn picksplit_layout() {
        assert_eq!(offset_of!(spgPickSplitIn, nTuples), 0);
        assert_eq!(offset_of!(spgPickSplitIn, datums), 8);
        assert_eq!(offset_of!(spgPickSplitIn, level), 16);
        assert_eq!(size_of::<spgPickSplitIn>(), 24);

        assert_eq!(offset_of!(spgPickSplitOut, hasPrefix), 0);
        assert_eq!(offset_of!(spgPickSplitOut, prefixDatum), 8);
        assert_eq!(offset_of!(spgPickSplitOut, nNodes), 16);
        assert_eq!(offset_of!(spgPickSplitOut, nodeLabels), 24);
        assert_eq!(offset_of!(spgPickSplitOut, mapTuplesToNodes), 32);
        assert_eq!(offset_of!(spgPickSplitOut, leafTupleDatums), 40);
        assert_eq!(size_of::<spgPickSplitOut>(), 48);
    }

    #[test]
    fn inner_consistent_layout() {
        assert_eq!(offset_of!(spgInnerConsistentIn, scankeys), 0);
        assert_eq!(offset_of!(spgInnerConsistentIn, orderbys), 8);
        assert_eq!(offset_of!(spgInnerConsistentIn, nkeys), 16);
        assert_eq!(offset_of!(spgInnerConsistentIn, norderbys), 20);
        assert_eq!(offset_of!(spgInnerConsistentIn, reconstructedValue), 24);
        assert_eq!(offset_of!(spgInnerConsistentIn, traversalValue), 32);
        assert_eq!(offset_of!(spgInnerConsistentIn, traversalMemoryContext), 40);
        assert_eq!(offset_of!(spgInnerConsistentIn, level), 48);
        assert_eq!(offset_of!(spgInnerConsistentIn, returnData), 52);
        assert_eq!(offset_of!(spgInnerConsistentIn, allTheSame), 53);
        assert_eq!(offset_of!(spgInnerConsistentIn, hasPrefix), 54);
        assert_eq!(offset_of!(spgInnerConsistentIn, prefixDatum), 56);
        assert_eq!(offset_of!(spgInnerConsistentIn, nNodes), 64);
        assert_eq!(offset_of!(spgInnerConsistentIn, nodeLabels), 72);
        assert_eq!(size_of::<spgInnerConsistentIn>(), 80);

        assert_eq!(offset_of!(spgInnerConsistentOut, nNodes), 0);
        assert_eq!(offset_of!(spgInnerConsistentOut, nodeNumbers), 8);
        assert_eq!(offset_of!(spgInnerConsistentOut, levelAdds), 16);
        assert_eq!(offset_of!(spgInnerConsistentOut, reconstructedValues), 24);
        assert_eq!(offset_of!(spgInnerConsistentOut, traversalValues), 32);
        assert_eq!(offset_of!(spgInnerConsistentOut, distances), 40);
        assert_eq!(size_of::<spgInnerConsistentOut>(), 48);
    }

    #[test]
    fn leaf_consistent_layout() {
        assert_eq!(offset_of!(spgLeafConsistentIn, scankeys), 0);
        assert_eq!(offset_of!(spgLeafConsistentIn, orderbys), 8);
        assert_eq!(offset_of!(spgLeafConsistentIn, nkeys), 16);
        assert_eq!(offset_of!(spgLeafConsistentIn, norderbys), 20);
        assert_eq!(offset_of!(spgLeafConsistentIn, reconstructedValue), 24);
        assert_eq!(offset_of!(spgLeafConsistentIn, traversalValue), 32);
        assert_eq!(offset_of!(spgLeafConsistentIn, level), 40);
        assert_eq!(offset_of!(spgLeafConsistentIn, returnData), 44);
        assert_eq!(offset_of!(spgLeafConsistentIn, leafDatum), 48);
        assert_eq!(size_of::<spgLeafConsistentIn>(), 56);

        assert_eq!(offset_of!(spgLeafConsistentOut, leafValue), 0);
        assert_eq!(offset_of!(spgLeafConsistentOut, recheck), 8);
        assert_eq!(offset_of!(spgLeafConsistentOut, recheckDistances), 9);
        assert_eq!(offset_of!(spgLeafConsistentOut, distances), 16);
        assert_eq!(size_of::<spgLeafConsistentOut>(), 24);
    }
}
