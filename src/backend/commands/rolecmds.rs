// :HACK: Preserve the historical root command path while role command logic
// lives in `pgrust_commands` and role-setting storage lives in
// `pgrust_catalog_store`.
pub use pgrust_catalog_store::role_settings::{role_settings, store_role_setting};
pub use pgrust_commands::rolecmds::*;
