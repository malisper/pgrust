//! Backend-local backing storage for the SSL GUC variables declared in
//! `be-secure.c` (the `conf->variable` targets that `guc_tables.c` wires up
//! with pointers into this translation unit).
//!
//! In C these are plain process globals (`char *ssl_cert_file;`,
//! `bool SSLPreferServerCiphers;`, `int ssl_min_protocol_version = ...;`),
//! read straight out of the GUC slot — none come from the ControlFile. Each
//! C backend owns its own copy, so each is a `thread_local!` cell here, with
//! a getter/setter pair the GUC machinery installs through
//! `GucVarAccessors`. Initial values mirror the C declarations
//! (string globals default to NULL, the two enums to their C initializers);
//! the GUC bootstrap then assigns each variable's `boot_val` through these
//! same accessors.

#![allow(non_upper_case_globals)]

use std::cell::RefCell;
use std::cell::Cell;

use be_secure_openssl::{PG_TLS1_2_VERSION, PG_TLS_ANY};

// String GUC backing cells. C `char *` globals; NULL stays distinguishable
// from empty (`Option<String>`), matching the `GucStringVar` contract.
thread_local! {
    /// `char *ssl_library;` (`be-secure.c`).
    static SSL_LIBRARY: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `char *ssl_cert_file;`
    static SSL_CERT_FILE: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `char *ssl_ca_file;`
    static SSL_CA_FILE: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `char *ssl_crl_file;`
    static SSL_CRL_FILE: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `char *ssl_crl_dir;`
    static SSL_CRL_DIR: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `char *ssl_dh_params_file;`
    static SSL_DH_PARAMS_FILE: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `char *ssl_passphrase_command;`
    static SSL_PASSPHRASE_COMMAND: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `char *SSLCipherSuites = NULL;` (`ssl_tls13_ciphers` GUC).
    static SSL_CIPHER_SUITES: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `char *SSLCipherList = NULL;` (`ssl_ciphers` GUC).
    static SSL_CIPHER_LIST: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `char *SSLECDHCurve;` (`ssl_groups` GUC).
    static SSL_ECDH_CURVE: RefCell<Option<String>> = const { RefCell::new(None) };
}

// Bool GUC backing cells.
thread_local! {
    /// `bool ssl_passphrase_command_supports_reload;`
    static SSL_PASSPHRASE_COMMAND_SUPPORTS_RELOAD: Cell<bool> = const { Cell::new(false) };
    /// `bool SSLPreferServerCiphers;` (`ssl_prefer_server_ciphers` GUC).
    static SSL_PREFER_SERVER_CIPHERS: Cell<bool> = const { Cell::new(false) };
}

// Enum GUC backing cells (stored as the `int` value the `config_enum`
// machinery uses). Initialized to the C declarations'
// `= PG_TLS1_2_VERSION` / `= PG_TLS_ANY`.
thread_local! {
    /// `int ssl_min_protocol_version = PG_TLS1_2_VERSION;`
    static SSL_MIN_PROTOCOL_VERSION: Cell<i32> = const { Cell::new(PG_TLS1_2_VERSION) };
    /// `int ssl_max_protocol_version = PG_TLS_ANY;`
    static SSL_MAX_PROTOCOL_VERSION: Cell<i32> = const { Cell::new(PG_TLS_ANY) };
}

macro_rules! string_accessors {
    ($cell:ident, $get:ident, $set:ident) => {
        #[inline]
        pub fn $get() -> Option<String> {
            $cell.with(|c| c.borrow().clone())
        }

        #[inline]
        pub fn $set(value: Option<String>) {
            $cell.with(|c| *c.borrow_mut() = value);
        }
    };
}

macro_rules! scalar_accessors {
    ($cell:ident, $get:ident, $set:ident, $ty:ty) => {
        #[inline]
        pub fn $get() -> $ty {
            $cell.with(|c| c.get())
        }

        #[inline]
        pub fn $set(value: $ty) {
            $cell.with(|c| c.set(value));
        }
    };
}

string_accessors!(SSL_LIBRARY, ssl_library, set_ssl_library);
string_accessors!(SSL_CERT_FILE, ssl_cert_file, set_ssl_cert_file);
string_accessors!(SSL_CA_FILE, ssl_ca_file, set_ssl_ca_file);
string_accessors!(SSL_CRL_FILE, ssl_crl_file, set_ssl_crl_file);
string_accessors!(SSL_CRL_DIR, ssl_crl_dir, set_ssl_crl_dir);
string_accessors!(SSL_DH_PARAMS_FILE, ssl_dh_params_file, set_ssl_dh_params_file);
string_accessors!(
    SSL_PASSPHRASE_COMMAND,
    ssl_passphrase_command,
    set_ssl_passphrase_command
);
string_accessors!(SSL_CIPHER_SUITES, ssl_cipher_suites, set_ssl_cipher_suites);
string_accessors!(SSL_CIPHER_LIST, ssl_cipher_list, set_ssl_cipher_list);
string_accessors!(SSL_ECDH_CURVE, ssl_ecdh_curve, set_ssl_ecdh_curve);

scalar_accessors!(
    SSL_PASSPHRASE_COMMAND_SUPPORTS_RELOAD,
    ssl_passphrase_command_supports_reload,
    set_ssl_passphrase_command_supports_reload,
    bool
);
scalar_accessors!(
    SSL_PREFER_SERVER_CIPHERS,
    ssl_prefer_server_ciphers,
    set_ssl_prefer_server_ciphers,
    bool
);
scalar_accessors!(
    SSL_MIN_PROTOCOL_VERSION,
    ssl_min_protocol_version,
    set_ssl_min_protocol_version,
    i32
);
scalar_accessors!(
    SSL_MAX_PROTOCOL_VERSION,
    ssl_max_protocol_version,
    set_ssl_max_protocol_version,
    i32
);
