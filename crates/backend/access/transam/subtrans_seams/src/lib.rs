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
