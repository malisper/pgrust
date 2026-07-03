#![allow(non_snake_case)]

use core::cell::Cell;

use types_error::PgResult;

// C's Session is all-NULL until GetSessionDsmHandle's first parallel query (unported).
thread_local! {
    static CURRENT_SESSION: Cell<bool> = const { Cell::new(false) };
}

pub fn InitializeSession() -> PgResult<()> {
    CURRENT_SESSION.set(true);
    Ok(())
}

pub fn CurrentSessionExists() -> bool {
    CURRENT_SESSION.get()
}

pub fn GetSessionDsmHandle() -> ! {
    panic!("GetSessionDsmHandle: parallel-worker session DSM unported (backend-access-common session.c)");
}

pub fn init_seams() {
    session_seams::initialize_session::set(InitializeSession);
}
