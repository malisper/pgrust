use types_error::PgResult;
use types_storage::SharedInvalidationMessage;

seam_core::seam!(
    // ReceiveSharedInvalidMessages(invalFunction, resetFunction) (sinval.c);
    // a handler error propagates out of the drain like C's ereport longjmp.
    pub fn receive_shared_invalid_messages(
        inval_function: &mut dyn FnMut(&SharedInvalidationMessage) -> PgResult<()>,
        reset_function: &mut dyn FnMut() -> PgResult<()>,
    ) -> PgResult<()>
);

seam_core::seam!(
    // SendSharedInvalidMessages(msgs, n) (sinval.c). Must not re-enter inval:
    // callers hold the inval state borrow across this call.
    pub fn send_shared_invalid_messages(msgs: &[SharedInvalidationMessage]) -> PgResult<()>
);


seam_core::seam!(
    // HandleCatchupInterrupt() (sinval.c); signal-handler-reachable, so the
    // implementation must be allocation-free.
    pub fn handle_catchup_interrupt()
);
