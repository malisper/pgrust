#![allow(dead_code, private_interfaces, unused_imports)]

mod backend;
mod include;
mod pgrust;
mod pl;
#[cfg(target_arch = "wasm32")]
pub mod wasm;

pub mod catalog {
    pub use crate::backend::catalog::store::CatalogWriteContext;
    pub use crate::backend::catalog::{CatalogError, CatalogStore, column_desc};
}

pub mod plpgsql {
    pub use crate::pl::plpgsql::*;
}

pub mod notices {
    pub use crate::backend::utils::misc::notices::*;
}

pub mod sql {
    pub use crate::backend::parser::*;
}

pub mod table_commands {
    pub use crate::backend::commands::tablecmds::{
        execute_delete_with_waiter, execute_insert, execute_truncate_table,
        execute_update_with_waiter,
    };
}

pub(crate) mod wire {
    pub(crate) use crate::backend::libpq::pqformat::{format_exec_error, infer_command_tag};
}

pub use backend::executor::{
    ExecError, ExecutorContext, exec_next, execute_readonly_statement, executor_catalog,
    executor_start,
};
pub use pgrust::cluster::Cluster;
pub use pgrust::database::{
    Database, DatabaseOpenOptions, DatabaseStatsStore, SelectGuard, Session, SessionStatsState,
};
pub use pgrust::server::serve;
pub use pgrust::session::ByteaOutputFormat;
pub use pgrust_core::{
    AttributeAlign, AttributeCompression, AttributeDesc, AttributeStorage, CompactString,
};
pub use pgrust_nodes::plannodes::PlanEstimate;
pub use pgrust_nodes::primnodes::{RelationDesc, TargetEntry};
pub use pgrust_nodes::{Expr, Plan, SessionReplicationRole, StatementResult, Value};
pub use pgrust_storage::buffer::*;
pub use pgrust_storage::include::storage::buf_internals::{
    BufferUsageStats, ClientId, FlushResult, RequestPageResult,
};
pub use pgrust_storage::smgr;
pub use pgrust_storage::smgr::*;

pub use smgr::{BLCKSZ, ForkNumber, RelFileLocator};
