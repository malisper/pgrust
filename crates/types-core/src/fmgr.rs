//! Fmgr-adjacent catalog limits (`pg_config_manual.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/fmgr.rs
//! as ports need items; only the items currently consumed are present.

pub const INDEX_MAX_KEYS: i32 = 32;
pub const NAMEDATALEN: i32 = 64;

/// `PG_VERSION_NUM` (`pg_config.h`) — numeric server version, 180003 for
/// PostgreSQL 18.3.
pub const PG_VERSION_NUM: i32 = 180_003;

/// `FLOAT8PASSBYVAL` (`c.h`) — `true` (1) on the 64-bit build platforms.
pub const FLOAT8PASSBYVAL: i32 = 1;

/// `FMGR_ABI_EXTRA` (`pg_config_manual.h`) — `"PostgreSQL"`, NUL-padded into
/// the 32-byte `abi_extra` field of a magic block.
pub const FMGR_ABI_EXTRA: [u8; 32] = [
    b'P', b'o', b's', b't', b'g', b'r', b'e', b'S', b'Q', b'L', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

/// `PG_MAGIC_FUNCTION_NAME_STRING` (`fmgr.h`) — the symbol a module exports to
/// provide its magic block (`dlsym` target in `internal_load_library`).
pub const PG_MAGIC_FUNCTION_NAME_STRING: &str = "Pg_magic_func";

/// `_PG_init` (`fmgr.h`) — the optional per-module initializer symbol.
pub const PG_INIT_FUNCTION_NAME_STRING: &str = "_PG_init";

/// `Pg_abi_values` (`fmgr.h`) — the values checked to verify ABI
/// compatibility of a dynamically loaded module. C compares these with
/// `memcmp`, so the struct deliberately has no padding; the port compares them
/// field-for-field (`internal_load_library`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgAbiValues {
    /// PostgreSQL major version (`PG_VERSION_NUM / 100`).
    pub version: i32,
    /// `FUNC_MAX_ARGS`.
    pub funcmaxargs: i32,
    /// `INDEX_MAX_KEYS`.
    pub indexmaxkeys: i32,
    /// `NAMEDATALEN`.
    pub namedatalen: i32,
    /// `FLOAT8PASSBYVAL`.
    pub float8byval: i32,
    /// `FMGR_ABI_EXTRA` — product-identity string, NUL-padded.
    pub abi_extra: [u8; 32],
}

impl PgAbiValues {
    /// `PG_MODULE_ABI_DATA` (`fmgr.h`) — the ABI values this server build
    /// requires a module to match.
    pub const fn server() -> Self {
        Self {
            version: PG_VERSION_NUM / 100,
            funcmaxargs: crate::primitive::FUNC_MAX_ARGS as i32,
            indexmaxkeys: INDEX_MAX_KEYS,
            namedatalen: NAMEDATALEN,
            float8byval: FLOAT8PASSBYVAL,
            abi_extra: FMGR_ABI_EXTRA,
        }
    }
}

/// `FmgrInfo` (`fmgr.h`), trimmed to the lookup key. C's struct caches the
/// resolved function pointer and call metadata; consumers here (e.g.
/// `ScanKeyInit`) only stamp `fn_oid` and defer the real fmgr lookup to the
/// code that invokes the function.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FmgrInfo {
    /// OID of the function (`pg_proc` OID).
    pub fn_oid: crate::primitive::Oid,
}

impl FmgrInfo {
    /// An unresolved `FmgrInfo` (`fn_oid = InvalidOid`).
    pub const fn empty() -> Self {
        Self { fn_oid: 0 }
    }
}

/// `F_INT4EQ` (`catalog/fmgroids.h`) — `int4eq`, pg_proc OID 65
/// (`pg_proc.dat`).
pub const F_INT4EQ: crate::primitive::RegProcedure = 65;
/// `F_OIDEQ` (`catalog/fmgroids.h`) — `oideq`, pg_proc OID 184
/// (`pg_proc.dat`).
pub const F_OIDEQ: crate::primitive::RegProcedure = 184;
