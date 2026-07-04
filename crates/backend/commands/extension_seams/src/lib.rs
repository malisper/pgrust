pub mod builtins;

use datum::Datum;
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};

seam_core::seam!(
    pub fn pg_available_extensions<'f, 'c>(
        flinfo: Option<&'f mut FmgrInfo>,
        fcinfo: &'c mut FunctionCallInfoBaseData,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    pub fn pg_available_extension_versions<'f, 'c>(
        flinfo: Option<&'f mut FmgrInfo>,
        fcinfo: &'c mut FunctionCallInfoBaseData,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    pub fn pg_extension_update_paths<'f, 'c>(
        flinfo: Option<&'f mut FmgrInfo>,
        fcinfo: &'c mut FunctionCallInfoBaseData,
    ) -> PgResult<Datum>
);
