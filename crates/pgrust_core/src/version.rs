//! Server version reported via the startup `ParameterStatus` packet, the
//! `server_version` / `server_version_num` GUCs, and the `version()` builtin.

/// Value reported for the `server_version` GUC and ParameterStatus.
pub const PG_VERSION_STRING: &str = "18.3";

/// Value reported for the `server_version_num` GUC. Encoded as
/// `major * 10000 + minor`, matching `PG_VERSION_NUM` upstream.
pub const PG_VERSION_NUM: i32 = 180003;

/// Server-side character set encoding. Reported via ParameterStatus and the
/// `server_encoding` GUC.
pub const SERVER_ENCODING: &str = "UTF8";
