//! Seam declarations for the `common-controldata-utils` unit
//! (`src/common/controldata_utils.c`): the `pg_control` file reader.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use ::control::ControlFileData;
use ::types_error::PgResult;

seam_core::seam!(
    /// `get_controlfile(DataDir, &crc_ok)` (controldata_utils.c): read and
    /// parse `global/pg_control` under `datadir`, returning the parsed
    /// `ControlFileData` and whether the stored CRC matched
    /// (`crc_ok`). I/O failures (open/read/short read) `ereport(ERROR)`
    /// (server build), so the surface is fallible.
    pub fn get_controlfile(datadir: &str) -> PgResult<(ControlFileData, bool)>
);
