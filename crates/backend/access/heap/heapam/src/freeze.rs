// heap_freeze_tuple (heapam.c), the standalone rewriteheap arm: pagefrz is
// pre-armed with freeze_required = true, so heap_tuple_should_freeze never
// runs. Multixact xmax is a loud panic (FreezeMultiXactId unported).
use types_core::xact::{
    TransactionIdIsNormal, TransactionIdIsValid, TransactionIdPrecedes,
};
use types_core::{InvalidTransactionId, TransactionId};
use types_tuple::htup::{
    HeapTupleHeaderData, HEAP_HOT_UPDATED, HEAP_KEYS_UPDATED, HEAP_MOVED_OFF, HEAP_XMAX_BITS,
    HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMIN_FROZEN,
};

const XLH_FREEZE_XVAC: u8 = 0x02;
const XLH_INVALID_XVAC: u8 = 0x04;

pub fn heap_freeze_tuple(
    tuple: &mut HeapTupleHeaderData,
    relfrozenxid: TransactionId,
    relminmxid: TransactionId,
    freeze_limit: TransactionId,
    cutoff_multi: TransactionId,
) -> bool {
    let _ = (relminmxid, cutoff_multi);

    let mut frz_xmax = tuple.xmax_raw();
    let mut frz_infomask2 = tuple.t_infomask2;
    let mut frz_infomask = tuple.t_infomask;
    let mut frzflags: u8 = 0;

    let mut freeze_xmin = false;
    let mut replace_xvac = false;
    let mut freeze_xmax = false;

    let xid = tuple.xmin_raw();
    if TransactionIdIsNormal(xid) {
        if TransactionIdPrecedes(xid, relfrozenxid) {
            panic!("found xmin {xid} from before relfrozenxid {relfrozenxid}");
        }
        // OldestXmin == FreezeLimit in this wrapper.
        freeze_xmin = TransactionIdPrecedes(xid, freeze_limit);
    }

    let xvac = tuple.xvac();
    if TransactionIdIsNormal(xvac) {
        replace_xvac = true;
    }

    let xid = frz_xmax;
    if tuple.t_infomask & HEAP_XMAX_IS_MULTI != 0 {
        panic!("unported: heapam.c FreezeMultiXactId (multixact xmax in heap_freeze_tuple)");
    } else if TransactionIdIsNormal(xid) {
        if TransactionIdPrecedes(xid, relfrozenxid) {
            panic!("found xmax {xid} from before relfrozenxid {relfrozenxid}");
        }
        freeze_xmax = TransactionIdPrecedes(xid, freeze_limit);
    } else if TransactionIdIsValid(xid) {
        panic!(
            "found raw xmax {xid} (infomask 0x{:04x}) not invalid and not multi",
            tuple.t_infomask
        );
    }

    if freeze_xmin {
        frz_infomask |= HEAP_XMIN_FROZEN;
    }
    if replace_xvac {
        if tuple.t_infomask & HEAP_MOVED_OFF != 0 {
            frzflags |= XLH_INVALID_XVAC;
        } else {
            frzflags |= XLH_FREEZE_XVAC;
        }
    }
    if freeze_xmax {
        frz_xmax = InvalidTransactionId;
        frz_infomask &= !HEAP_XMAX_BITS;
        frz_infomask |= HEAP_XMAX_INVALID;
        frz_infomask2 &= !HEAP_HOT_UPDATED;
        frz_infomask2 &= !HEAP_KEYS_UPDATED;
    }

    let do_freeze = freeze_xmin || replace_xvac || freeze_xmax;
    if do_freeze {
        tuple.set_xmax(frz_xmax);
        if frzflags & XLH_FREEZE_XVAC != 0 {
            tuple.set_xvac(types_core::xact::FrozenTransactionId);
        }
        if frzflags & XLH_INVALID_XVAC != 0 {
            tuple.set_xvac(InvalidTransactionId);
        }
        tuple.t_infomask = frz_infomask;
        tuple.t_infomask2 = frz_infomask2;
    }
    do_freeze
}
