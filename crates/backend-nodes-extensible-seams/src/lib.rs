//! Seam declarations for the `backend-nodes-extensible` unit
//! (`nodes/extensible.c`): the extension-defined-node and custom-scan registry.
//!
//! The copy/equal/out/read node dispatch (`copyfuncs.c`/`equalfuncs.c`/
//! `outfuncs.c`/`readfuncs.c`) and the custom-scan executor
//! (`nodeCustom.c`) look these tables up across a cycle. The owning crate
//! installs all four from its `init_seams()`; until then a call panics loudly.
//!
//! Method tables and the `extnodename`/`CustomName` keys cross the boundary as
//! the C raw-pointer ABI (`const char *`, `const ExtensibleNodeMethods *`,
//! `const CustomScanMethods *`), matching `extensible.h`'s declarations.

#![allow(non_snake_case)]

use core::ffi::c_char;

use types_error::PgResult;
use types_extensible::{CustomScanMethods, ExtensibleNodeMethods};

seam_core::seam!(
    /// `RegisterExtensibleNodeMethods(const ExtensibleNodeMethods *methods)`
    /// (extensible.c): register a new type of extensible node.
    pub fn RegisterExtensibleNodeMethods(methods: *const ExtensibleNodeMethods) -> PgResult<()>
);

seam_core::seam!(
    /// `RegisterCustomScanMethods(const CustomScanMethods *methods)`
    /// (extensible.c): register a new type of custom scan node.
    pub fn RegisterCustomScanMethods(methods: *const CustomScanMethods) -> PgResult<()>
);

seam_core::seam!(
    /// `GetExtensibleNodeMethods(const char *extnodename, bool missing_ok)`
    /// (extensible.c): look up an extensible-node method table by name. NULL
    /// when `missing_ok` and not found, else `ERRCODE_UNDEFINED_OBJECT`.
    pub fn GetExtensibleNodeMethods(
        extnodename: *const c_char,
        missing_ok: bool,
    ) -> PgResult<*const ExtensibleNodeMethods>
);

seam_core::seam!(
    /// `GetCustomScanMethods(const char *CustomName, bool missing_ok)`
    /// (extensible.c): look up a custom-scan method table by name. NULL when
    /// `missing_ok` and not found, else `ERRCODE_UNDEFINED_OBJECT`.
    pub fn GetCustomScanMethods(
        CustomName: *const c_char,
        missing_ok: bool,
    ) -> PgResult<*const CustomScanMethods>
);
