use core::ffi::{c_char, c_int, c_short, c_uchar, c_void};

use crate::{Datum, MemoryContext, NodeTag, Oid, StringInfoData};

pub const PG_VERSION_NUM: c_int = 180_003;
pub const FUNC_MAX_ARGS: c_int = 100;
pub const INDEX_MAX_KEYS: c_int = 32;
pub const NAMEDATALEN: c_int = 64;
pub const FLOAT8PASSBYVAL: c_int = 1;
pub const FMGR_ABI_EXTRA: [c_char; 32] = [
    b'P' as c_char,
    b'o' as c_char,
    b's' as c_char,
    b't' as c_char,
    b'g' as c_char,
    b'r' as c_char,
    b'e' as c_char,
    b'S' as c_char,
    b'Q' as c_char,
    b'L' as c_char,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
];
pub const PG_MAGIC_FUNCTION_NAME_STRING: &str = "Pg_magic_func";
pub const PG_INIT_FUNCTION_NAME_STRING: &str = "_PG_init";

pub type fmNodePtr = *mut Node;
pub type fmStringInfo = *mut StringInfoData;
pub type FunctionCallInfo = *mut FunctionCallInfoBaseData;
pub type PGFunction = Option<unsafe extern "C" fn(FunctionCallInfo) -> Datum>;
pub type PGModuleMagicFunction = Option<unsafe extern "C" fn() -> *const Pg_magic_struct>;

pub const TRACK_FUNC_OFF: c_uchar = 0;
pub const TRACK_FUNC_PL: c_uchar = 1;
pub const TRACK_FUNC_ALL: c_uchar = 2;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pg_abi_values {
    pub version: c_int,
    pub funcmaxargs: c_int,
    pub indexmaxkeys: c_int,
    pub namedatalen: c_int,
    pub float8byval: c_int,
    pub abi_extra: [c_char; 32],
}

impl Pg_abi_values {
    pub const fn server() -> Self {
        Self {
            version: PG_VERSION_NUM / 100,
            funcmaxargs: FUNC_MAX_ARGS,
            indexmaxkeys: INDEX_MAX_KEYS,
            namedatalen: NAMEDATALEN,
            float8byval: FLOAT8PASSBYVAL,
            abi_extra: FMGR_ABI_EXTRA,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Pg_magic_struct {
    pub len: c_int,
    pub abi_fields: Pg_abi_values,
    pub name: *const c_char,
    pub version: *const c_char,
}

#[repr(C)]
pub struct DynamicFileList {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Node {
    pub type_: NodeTag,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct varlena {
    pub vl_len_: [c_char; 4],
    pub vl_dat: [c_char; 0],
}

pub type bytea = varlena;
pub type text = varlena;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NullableDatum {
    pub value: Datum,
    pub isnull: bool,
}

impl NullableDatum {
    pub const fn value(value: Datum) -> Self {
        Self {
            value,
            isnull: false,
        }
    }

    pub const fn null() -> Self {
        Self {
            value: 0,
            isnull: true,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FunctionCallInfoBaseData {
    pub flinfo: *mut FmgrInfo,
    pub context: fmNodePtr,
    pub resultinfo: fmNodePtr,
    pub fncollation: Oid,
    pub isnull: bool,
    pub nargs: c_short,
    pub args: [NullableDatum; 0],
}

impl FunctionCallInfoBaseData {
    pub const fn new(
        flinfo: *mut FmgrInfo,
        nargs: c_short,
        fncollation: Oid,
        context: fmNodePtr,
        resultinfo: fmNodePtr,
    ) -> Self {
        Self {
            flinfo,
            context,
            resultinfo,
            fncollation,
            isnull: false,
            nargs,
            args: [],
        }
    }

    pub fn nargs(&self) -> usize {
        self.nargs.max(0) as usize
    }

    pub fn set_result_null(&mut self, isnull: bool) {
        self.isnull = isnull;
    }

    pub fn result_is_null(&self) -> bool {
        self.isnull
    }

    /// Return an argument from C-provided trailing storage.
    ///
    /// # Safety
    ///
    /// `self` must point to a complete PostgreSQL `FunctionCallInfoBaseData`
    /// allocation with enough trailing `NullableDatum` entries for `nargs`.
    pub unsafe fn arg(&self, index: usize) -> Option<NullableDatum> {
        if index >= self.nargs() {
            return None;
        }

        let args = core::ptr::addr_of!(self.args).cast::<NullableDatum>();
        Some(unsafe { *args.add(index) })
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FmgrInfo {
    pub fn_addr: PGFunction,
    pub fn_oid: Oid,
    pub fn_nargs: c_short,
    pub fn_strict: bool,
    pub fn_retset: bool,
    pub fn_stats: c_uchar,
    pub fn_extra: *mut c_void,
    pub fn_mcxt: MemoryContext,
    pub fn_expr: fmNodePtr,
}

impl FmgrInfo {
    pub const fn empty() -> Self {
        Self {
            fn_addr: None,
            fn_oid: 0,
            fn_nargs: 0,
            fn_strict: false,
            fn_retset: false,
            fn_stats: TRACK_FUNC_OFF,
            fn_extra: core::ptr::null_mut(),
            fn_mcxt: core::ptr::null_mut(),
            fn_expr: core::ptr::null_mut(),
        }
    }

    pub fn set_expr(&mut self, expr: fmNodePtr) {
        self.fn_expr = expr;
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Pg_finfo_record {
    pub api_version: c_int,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FmgrBuiltin {
    pub foid: Oid,
    pub funcName: *const c_char,
    pub nargs: c_short,
    pub strict: bool,
    pub retset: bool,
    pub func: PGFunction,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PgAbiValues {
    pub version: c_int,
    pub funcmaxargs: c_int,
    pub indexmaxkeys: c_int,
    pub namedatalen: c_int,
    pub float8byval: c_int,
    pub abi_extra: [c_char; 32],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PgMagicStruct {
    pub len: c_int,
    pub abi_fields: PgAbiValues,
    pub name: *const c_char,
    pub version: *const c_char,
}

unsafe impl Sync for PgMagicStruct {}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn fmgr_layout_matches_postgres_abi_on_64_bit() {
        assert_eq!(size_of::<NullableDatum>(), 16);
        assert_eq!(align_of::<NullableDatum>(), 8);

        assert_eq!(offset_of!(FunctionCallInfoBaseData, flinfo), 0);
        assert_eq!(offset_of!(FunctionCallInfoBaseData, context), 8);
        assert_eq!(offset_of!(FunctionCallInfoBaseData, resultinfo), 16);
        assert_eq!(offset_of!(FunctionCallInfoBaseData, fncollation), 24);
        assert_eq!(offset_of!(FunctionCallInfoBaseData, isnull), 28);
        assert_eq!(offset_of!(FunctionCallInfoBaseData, nargs), 30);
        assert_eq!(offset_of!(FunctionCallInfoBaseData, args), 32);
        assert_eq!(size_of::<FunctionCallInfoBaseData>(), 32);

        assert_eq!(offset_of!(FmgrInfo, fn_addr), 0);
        assert_eq!(offset_of!(FmgrInfo, fn_oid), 8);
        assert_eq!(offset_of!(FmgrInfo, fn_nargs), 12);
        assert_eq!(offset_of!(FmgrInfo, fn_strict), 14);
        assert_eq!(offset_of!(FmgrInfo, fn_retset), 15);
        assert_eq!(offset_of!(FmgrInfo, fn_stats), 16);
        assert_eq!(offset_of!(FmgrInfo, fn_extra), 24);
        assert_eq!(offset_of!(FmgrInfo, fn_mcxt), 32);
        assert_eq!(offset_of!(FmgrInfo, fn_expr), 40);
        assert_eq!(size_of::<FmgrInfo>(), 48);
    }

    #[test]
    fn magic_layout_matches_extension_abi() {
        assert_eq!(size_of::<PgAbiValues>(), 52);
        assert_eq!(align_of::<PgAbiValues>(), 4);
        assert_eq!(size_of::<PgMagicStruct>(), 72);
        assert_eq!(align_of::<PgMagicStruct>(), 8);
    }
}
