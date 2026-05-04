// :HACK: Preserve the historical root command path while CREATE SCHEMA command
// logic lives in `pgrust_commands`.
pub use pgrust_commands::schemacmds::*;
