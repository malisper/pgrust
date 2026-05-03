use crate::backend::access::transam::xact::{
    CommandId, FROZEN_TRANSACTION_ID, INVALID_TRANSACTION_ID, TransactionId, TransactionManager,
    TransactionStatus,
};
use crate::backend::utils::time::snapmgr::Snapshot;
use crate::include::access::htup::{
    HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID, HeapTuple,
    INFOMASK_OFFSET,
};

pub trait SnapshotVisibility {
    fn tuple_bytes_try_visible_from_hints(&self, bytes: &[u8]) -> Option<bool>;
    fn tuple_bytes_visible(&self, txns: &TransactionManager, bytes: &[u8]) -> bool;
    fn tuple_bytes_visible_with_hints(
        &self,
        txns: &TransactionManager,
        bytes: &[u8],
    ) -> (bool, u16);
    fn tuple_visible(&self, txns: &TransactionManager, tuple: &HeapTuple) -> bool;

    #[cfg(test)]
    fn check_visibility(
        &self,
        txns: &TransactionManager,
        xmin: TransactionId,
        xmax: TransactionId,
        cid: CommandId,
    ) -> bool;
}

impl SnapshotVisibility for Snapshot {
    fn tuple_bytes_try_visible_from_hints(&self, bytes: &[u8]) -> Option<bool> {
        let infomask = u16::from_le_bytes([bytes[INFOMASK_OFFSET], bytes[INFOMASK_OFFSET + 1]]);
        let xmin = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        if infomask & HEAP_XMIN_COMMITTED != 0 {
            if xmin >= self.xmax {
                return Some(false);
            }
            if self.transaction_active_in_snapshot(xmin) {
                return Some(false);
            }
            if infomask & HEAP_XMAX_INVALID != 0 {
                return Some(true);
            }
            if infomask & HEAP_XMAX_COMMITTED != 0 {
                let xmax = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
                if xmax >= self.xmax {
                    return Some(true);
                }
                return Some(self.transaction_active_in_snapshot(xmax));
            }
            return None;
        }
        if infomask & HEAP_XMIN_INVALID != 0 {
            return Some(false);
        }
        None
    }

    fn tuple_bytes_visible(&self, txns: &TransactionManager, bytes: &[u8]) -> bool {
        let infomask = u16::from_le_bytes([bytes[INFOMASK_OFFSET], bytes[INFOMASK_OFFSET + 1]]);
        let xmin = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        if infomask & HEAP_XMIN_COMMITTED != 0 {
            if xmin >= self.xmax {
                return false;
            }
            if self.transaction_active_in_snapshot(xmin) {
                return false;
            }
            if infomask & HEAP_XMAX_INVALID != 0 {
                return true;
            }
            if infomask & HEAP_XMAX_COMMITTED != 0 {
                let xmax = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
                if xmax >= self.xmax {
                    return true;
                }
                return self.transaction_active_in_snapshot(xmax);
            }
        }
        if infomask & HEAP_XMIN_INVALID != 0 {
            return false;
        }

        let xmax = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let cid = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        check_visibility(self, txns, xmin, xmax, cid)
    }

    fn tuple_bytes_visible_with_hints(
        &self,
        txns: &TransactionManager,
        bytes: &[u8],
    ) -> (bool, u16) {
        let infomask = u16::from_le_bytes([bytes[INFOMASK_OFFSET], bytes[INFOMASK_OFFSET + 1]]);
        let xmin = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        if infomask & HEAP_XMIN_COMMITTED != 0 {
            if xmin >= self.xmax {
                return (false, 0);
            }
            if self.transaction_active_in_snapshot(xmin) {
                return (false, 0);
            }
            if infomask & HEAP_XMAX_INVALID != 0 {
                return (true, 0);
            }
            if infomask & HEAP_XMAX_COMMITTED != 0 {
                let xmax = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
                if xmax >= self.xmax {
                    return (true, 0);
                }
                return (self.transaction_active_in_snapshot(xmax), 0);
            }
        }
        if infomask & HEAP_XMIN_INVALID != 0 {
            return (false, 0);
        }

        let xmax = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let cid = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let visible = check_visibility(self, txns, xmin, xmax, cid);

        let mut hints: u16 = 0;
        let _xmin_settled = if infomask & (HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID) != 0 {
            true
        } else if xmin == INVALID_TRANSACTION_ID || xmin == self.current_xid {
            false
        } else {
            match txns.status(xmin) {
                Some(TransactionStatus::Committed) => {
                    hints |= HEAP_XMIN_COMMITTED;
                    true
                }
                Some(TransactionStatus::Aborted) => {
                    hints |= HEAP_XMIN_INVALID;
                    true
                }
                _ => false,
            }
        };

        let xmin_known_committed =
            (infomask & HEAP_XMIN_COMMITTED != 0) || (hints & HEAP_XMIN_COMMITTED != 0);
        if xmin_known_committed
            && infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID) == 0
            && xmax != INVALID_TRANSACTION_ID
            && xmax != self.current_xid
        {
            match txns.status(xmax) {
                Some(TransactionStatus::Committed) => hints |= HEAP_XMAX_COMMITTED,
                Some(TransactionStatus::Aborted) => hints |= HEAP_XMAX_INVALID,
                _ => {}
            }
        }

        (visible, hints)
    }

    fn tuple_visible(&self, txns: &TransactionManager, tuple: &HeapTuple) -> bool {
        check_visibility(
            self,
            txns,
            tuple.header.xmin,
            tuple.header.xmax,
            tuple.header.cid_or_xvac,
        )
    }

    #[cfg(test)]
    fn check_visibility(
        &self,
        txns: &TransactionManager,
        xmin: TransactionId,
        xmax: TransactionId,
        cid: CommandId,
    ) -> bool {
        check_visibility(self, txns, xmin, xmax, cid)
    }
}

fn check_visibility(
    snapshot: &Snapshot,
    txns: &TransactionManager,
    xmin: TransactionId,
    xmax: TransactionId,
    cid: CommandId,
) -> bool {
    if xmin == INVALID_TRANSACTION_ID || xmin == FROZEN_TRANSACTION_ID {
        if xmax == INVALID_TRANSACTION_ID {
            return true;
        }
        if xmax == snapshot.current_xid {
            return false;
        }
        if xmax >= snapshot.xmax {
            return true;
        }
        if snapshot.transaction_active_in_snapshot(xmax) {
            return true;
        }
        return match txns.status(xmax) {
            Some(TransactionStatus::Committed) => false,
            Some(TransactionStatus::Aborted) | Some(TransactionStatus::InProgress) | None => true,
        };
    }
    if snapshot.transaction_is_own(xmin) {
        let (cmin, cmax) = own_tuple_command_ids(txns, xmin, cid);
        if cmin >= snapshot.current_cid {
            return false;
        }
        if xmax == INVALID_TRANSACTION_ID {
            return true;
        }
        if snapshot.transaction_is_own(xmax) {
            return cmax >= snapshot.current_cid;
        }
        if xmax >= snapshot.xmax {
            return true;
        }
        if snapshot.transaction_active_in_snapshot(xmax) {
            return true;
        }
        return match txns.status(xmax) {
            Some(TransactionStatus::Committed) => false,
            Some(TransactionStatus::Aborted) | Some(TransactionStatus::InProgress) | None => true,
        };
    }
    if xmin >= snapshot.xmax {
        return false;
    }
    if snapshot.transaction_active_in_snapshot(xmin) {
        return false;
    }
    match txns.status(xmin) {
        Some(TransactionStatus::Committed) => {}
        Some(TransactionStatus::Aborted) | Some(TransactionStatus::InProgress) | None => {
            return false;
        }
    }

    if xmax == INVALID_TRANSACTION_ID {
        return true;
    }
    if snapshot.transaction_is_own(xmax) {
        return cid >= snapshot.current_cid;
    }
    if xmax >= snapshot.xmax {
        return true;
    }
    if snapshot.transaction_active_in_snapshot(xmax) {
        return true;
    }
    match txns.status(xmax) {
        Some(TransactionStatus::Committed) => false,
        Some(TransactionStatus::Aborted) | Some(TransactionStatus::InProgress) | None => true,
    }
}

fn own_tuple_command_ids(
    txns: &TransactionManager,
    xmin: TransactionId,
    cid: CommandId,
) -> (CommandId, CommandId) {
    txns.combo_command_pair(xmin, cid).unwrap_or((cid, cid))
}
