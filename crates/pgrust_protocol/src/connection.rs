use std::collections::HashMap;

use crate::copy::CopyInState;
use crate::extended::PreparedStatement;

#[derive(Debug)]
pub struct ConnectionState<S, C> {
    pub session: S,
    pub prepared: HashMap<String, PreparedStatement>,
    pub portals: HashMap<String, ()>,
    pub copy_in: Option<CopyInState<C>>,
    pub ignore_till_sync: bool,
    pub extended_segment_command_count: usize,
    pub pipeline_implicit_txn: bool,
}

impl<S, C> ConnectionState<S, C> {
    pub fn new(session: S) -> Self {
        Self {
            session,
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
            ignore_till_sync: false,
            extended_segment_command_count: 0,
            pipeline_implicit_txn: false,
        }
    }
}
