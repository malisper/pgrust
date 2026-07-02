//! Output-decision policy. Every function here is allocation-free, seam-free,
//! and inlineable: the per-statement hot path.

use ::types_dest::CommandDest;
use ::types_error::{ErrorLevel, ERROR, FATAL, INFO, LOG, LOG_SERVER_ONLY, WARNING_CLIENT_ONLY};

use crate::config;

#[inline]
pub fn is_log_level_output(elevel: ErrorLevel, log_min_level: ErrorLevel) -> bool {
    if elevel == LOG || elevel == LOG_SERVER_ONLY {
        if log_min_level == LOG || log_min_level <= ERROR {
            return true;
        }
    } else if elevel == WARNING_CLIENT_ONLY {
        // never sent to log, regardless of log_min_level
        return false;
    } else if log_min_level == LOG {
        if elevel >= FATAL {
            return true;
        }
    } else if elevel >= log_min_level {
        return true;
    }

    false
}

#[inline]
pub fn should_output_to_server(elevel: ErrorLevel) -> bool {
    is_log_level_output(elevel, config::log_min_messages())
}

#[inline]
pub fn should_output_to_client(elevel: ErrorLevel) -> bool {
    if config::where_to_send_output() == CommandDest::Remote && elevel != LOG_SERVER_ONLY {
        if config::client_auth_in_progress() {
            elevel >= ERROR
        } else {
            elevel >= config::client_min_messages() || elevel == INFO
        }
    } else {
        false
    }
}

#[inline]
pub fn message_level_is_interesting(elevel: ErrorLevel) -> bool {
    elevel >= ERROR || should_output_to_server(elevel) || should_output_to_client(elevel)
}
