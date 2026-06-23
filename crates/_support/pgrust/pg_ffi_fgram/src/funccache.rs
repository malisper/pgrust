use core::ffi::{c_char, c_int, c_void};

use crate::{
    uint32, uint64, Bitmapset, FmgrInfo, FunctionCallInfo, HeapTuple, ItemPointerData,
    MemoryContext, NameData, NodeTag, Oid, Relation, Size, TransactionId, TupleDesc,
    TupleTableSlot, Tuplestorestate, FUNC_MAX_ARGS,
};

pub const PROCOID: Oid = 47;
/// `PROCNAMEARGSNSP` SysCacheIdentifier (catalog/syscache_ids.h).
pub const PROCNAMEARGSNSP: c_int = 46;

/// `Anum_pg_proc_*` column numbers (catalog/pg_proc_d.h).
pub const Anum_pg_proc_proargmodes: c_int = 22;
pub const Anum_pg_proc_proargnames: c_int = 23;
pub const Anum_pg_proc_prosrc: c_int = 26;
pub const Anum_pg_proc_prosqlbody: c_int = 28;

pub const PROARGMODE_IN: c_char = b'i' as c_char;
pub const PROARGMODE_OUT: c_char = b'o' as c_char;
pub const PROARGMODE_TABLE: c_char = b't' as c_char;

pub const T_EventTriggerData: NodeTag = 441;
pub const T_TriggerData: NodeTag = 442;

pub const ANYARRAYOID: Oid = 2277;
pub const ANYELEMENTOID: Oid = 2283;
pub const ANYNONARRAYOID: Oid = 2776;
pub const ANYENUMOID: Oid = 3500;
pub const ANYRANGEOID: Oid = 3831;
pub const ANYMULTIRANGEOID: Oid = 4537;
pub const ANYCOMPATIBLEOID: Oid = 5077;
pub const ANYCOMPATIBLEARRAYOID: Oid = 5078;
pub const ANYCOMPATIBLENONARRAYOID: Oid = 5079;
pub const ANYCOMPATIBLERANGEOID: Oid = 5080;
pub const ANYCOMPATIBLEMULTIRANGEOID: Oid = 4538;

pub const INT4ARRAYOID: Oid = 1007;
pub const INT4RANGEOID: Oid = 3904;
pub const INT4MULTIRANGEOID: Oid = 4451;
pub const RECORDARRAYOID: Oid = 2287;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct oidvector {
    pub vl_len_: i32,
    pub ndim: c_int,
    pub dataoffset: i32,
    pub elemtype: Oid,
    pub dim1: c_int,
    pub lbound1: c_int,
    pub values: [Oid; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_proc {
    pub oid: Oid,
    pub proname: NameData,
    pub pronamespace: Oid,
    pub proowner: Oid,
    pub prolang: Oid,
    pub procost: f32,
    pub prorows: f32,
    pub provariadic: Oid,
    pub prosupport: Oid,
    pub prokind: c_char,
    pub prosecdef: bool,
    pub proleakproof: bool,
    pub proisstrict: bool,
    pub proretset: bool,
    pub provolatile: c_char,
    pub proparallel: c_char,
    pub pronargs: i16,
    pub pronargdefaults: i16,
    pub prorettype: Oid,
    pub proargtypes: oidvector,
}

pub type Form_pg_proc = *mut FormData_pg_proc;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Trigger {
    pub tgoid: Oid,
    pub tgname: *mut c_char,
    pub tgfoid: Oid,
    pub tgtype: i16,
    pub tgenabled: c_char,
    pub tgisinternal: bool,
    pub tgisclone: bool,
    pub tgconstrrelid: Oid,
    pub tgconstrindid: Oid,
    pub tgconstraint: Oid,
    pub tgdeferrable: bool,
    pub tginitdeferred: bool,
    pub tgnargs: i16,
    pub tgnattr: i16,
    pub tgattr: *mut i16,
    pub tgargs: *mut *mut c_char,
    pub tgqual: *mut c_char,
    pub tgoldtable: *mut c_char,
    pub tgnewtable: *mut c_char,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TriggerData {
    pub type_: NodeTag,
    pub tg_event: uint32,
    pub tg_relation: Relation,
    pub tg_trigtuple: HeapTuple,
    pub tg_newtuple: HeapTuple,
    pub tg_trigger: *mut Trigger,
    pub tg_trigslot: *mut TupleTableSlot,
    pub tg_newslot: *mut TupleTableSlot,
    pub tg_oldtable: *mut Tuplestorestate,
    pub tg_newtable: *mut Tuplestorestate,
    pub tg_updatedcols: *const Bitmapset,
}

pub type CachedFunctionDeleteCallback = Option<unsafe extern "C" fn(*mut CachedFunction)>;
pub type CachedFunctionCompileCallback = Option<
    unsafe extern "C" fn(
        FunctionCallInfo,
        HeapTuple,
        *const CachedFunctionHashKey,
        *mut CachedFunction,
        bool,
    ),
>;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CachedFunctionHashKey {
    pub funcOid: Oid,
    pub isTrigger: bool,
    pub isEventTrigger: bool,
    pub cacheEntrySize: Size,
    pub trigOid: Oid,
    pub inputCollation: Oid,
    pub nargs: c_int,
    pub callResultType: TupleDesc,
    pub argtypes: [Oid; FUNC_MAX_ARGS as usize],
}

impl Default for CachedFunctionHashKey {
    fn default() -> Self {
        Self {
            funcOid: 0,
            isTrigger: false,
            isEventTrigger: false,
            cacheEntrySize: 0,
            trigOid: 0,
            inputCollation: 0,
            nargs: 0,
            callResultType: core::ptr::null_mut(),
            argtypes: [0; FUNC_MAX_ARGS as usize],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CachedFunction {
    pub fn_hashkey: *mut CachedFunctionHashKey,
    pub fn_xmin: TransactionId,
    pub fn_tid: ItemPointerData,
    pub dcallback: CachedFunctionDeleteCallback,
    pub use_count: uint64,
}

impl Default for CachedFunction {
    fn default() -> Self {
        Self {
            fn_hashkey: core::ptr::null_mut(),
            fn_xmin: 0,
            fn_tid: ItemPointerData::default(),
            dcallback: None,
            use_count: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CachedFunctionHashEntry {
    pub key: CachedFunctionHashKey,
    pub function: *mut CachedFunction,
}

#[repr(C)]
pub struct FuncCacheOpaque {
    _private: [u8; 0],
    _marker: core::marker::PhantomData<(*mut c_void, *mut FmgrInfo, *mut MemoryContext)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn cached_function_key_layout_matches_postgres() {
        assert_eq!(size_of::<CachedFunctionHashKey>(), 440);
        assert_eq!(align_of::<CachedFunctionHashKey>(), 8);
        assert_eq!(offset_of!(CachedFunctionHashKey, callResultType), 32);
        assert_eq!(offset_of!(CachedFunctionHashKey, argtypes), 40);
    }

    #[test]
    fn cached_function_layout_matches_postgres() {
        assert_eq!(size_of::<CachedFunction>(), 40);
        assert_eq!(align_of::<CachedFunction>(), 8);
        assert_eq!(offset_of!(CachedFunction, fn_hashkey), 0);
        assert_eq!(offset_of!(CachedFunction, fn_xmin), 8);
        assert_eq!(offset_of!(CachedFunction, fn_tid), 12);
        assert_eq!(offset_of!(CachedFunction, dcallback), 24);
        assert_eq!(offset_of!(CachedFunction, use_count), 32);
    }
}
