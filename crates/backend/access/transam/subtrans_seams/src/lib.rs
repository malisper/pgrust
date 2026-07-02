seam_core::seam!(
    pub fn sub_trans_get_topmost_transaction(
        xid: types_core::TransactionId,
    ) -> types_error::PgResult<types_core::TransactionId>
);

seam_core::seam!(
    pub fn sub_trans_set_parent(
        xid: types_core::TransactionId,
        parent: types_core::TransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    // ExtendSUBTRANS (subtrans.c); GetNewTransactionId's per-assignment call.
    // Direct dep would cycle: varsup -> subtrans -> procarray -> varsup.
    pub fn extend_subtrans(newest_xact: types_core::TransactionId) -> types_error::PgResult<()>
);

seam_core::seam!(
    // StartupSUBTRANS(oldestActiveXID) (subtrans.c).
    pub fn startup_subtrans(oldest_active_xid: types_core::TransactionId) -> types_error::PgResult<()>
);

seam_core::seam!(
    // CheckPointSUBTRANS() (subtrans.c).
    pub fn check_point_subtrans() -> types_error::PgResult<()>
);

seam_core::seam!(
    // TruncateSUBTRANS(oldestXact) (subtrans.c).
    pub fn truncate_subtrans(oldest_xact: types_core::TransactionId) -> types_error::PgResult<()>
);
