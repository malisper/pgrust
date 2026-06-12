//! Output-decision policy: is_log_level_output and friends.

use types_dest::DestRemote;
use types_error::{ErrorLevel, ERROR, FATAL, INFO, LOG, LOG_SERVER_ONLY, WARNING_CLIENT_ONLY};

use crate::config;

/// `is_log_level_output` — is elevel logically >= log_min_level?
///
/// LOG/LOG_SERVER_ONLY sort out-of-order, between ERROR and FATAL; that is
/// the right thing for testing whether a message should go to the postmaster
/// log, whereas a simple >= test is correct for the client side.
pub fn is_log_level_output(elevel: ErrorLevel, log_min_level: ErrorLevel) -> bool {
    if elevel == LOG || elevel == LOG_SERVER_ONLY {
        if log_min_level == LOG || log_min_level <= ERROR {
            return true;
        }
    } else if elevel == WARNING_CLIENT_ONLY {
        // never sent to log, regardless of log_min_level
        return false;
    } else if log_min_level == LOG {
        // elevel != LOG
        if elevel >= FATAL {
            return true;
        }
    }
    // Neither is LOG
    else if elevel >= log_min_level {
        return true;
    }

    false
}

/// `should_output_to_server` — should a message of this elevel go to the log?
pub fn should_output_to_server(elevel: ErrorLevel) -> bool {
    is_log_level_output(elevel, config::log_min_messages())
}

/// `should_output_to_client` — should a message of this elevel go to the
/// client? `client_min_messages` is honored only after the authentication
/// handshake completes (security, and clients that can't handle NOTICE during
/// auth); INFO is always sent.
pub fn should_output_to_client(elevel: ErrorLevel) -> bool {
    if config::where_to_send_output() == DestRemote && elevel != LOG_SERVER_ONLY {
        if config::client_auth_in_progress() {
            elevel >= ERROR
        } else {
            elevel >= config::client_min_messages() || elevel == INFO
        }
    } else {
        false
    }
}

/// `message_level_is_interesting` — would ereport/elog with this elevel be a
/// no-op? Kept in sync with the decision-making in errstart.
pub fn message_level_is_interesting(elevel: ErrorLevel) -> bool {
    elevel >= ERROR || should_output_to_server(elevel) || should_output_to_client(elevel)
}
