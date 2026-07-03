// namespace.c, lookup spine only: RangeVarGetRelidExtended resolution,
// search_path recomputation + SearchPathCache, SearchPathMatcher, and the
// temp-namespace predicates. Deferred loud (named panics): temp-namespace
// creation (AccessTempTableNamespace/InitTempTableNamespace/RemoveTempRelations
// and the AtEOXact lifecycle it arms), the creation/DDL half
// (RangeVarGetCreationNamespace family, CheckSetNamespace), and the
// non-relation object lookups except OpernameGetOprid (Funcname/
// OpernameGetCandidates/Type/visibility family).
// C divergences: objectaccess is unported, so InvokeNamespaceSearchHook passes
// and object_access_hook is never set (forceRecompute stays false); the
// active*/base* variable pairs always alias after PG16, so one set is kept.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;

use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{
    InvalidOid, InvalidSubTransactionId, Oid, ProcNumber, SubTransactionId, INVALID_PROC_NUMBER,
};
use types_error::PgResult;

pub mod builtins;
mod lookup;
mod path;
mod temp;
#[cfg(test)]
mod tests;
mod visibility;

pub use temp::{
    AccessTempTableNamespace, GetTempTableNamespace, RangeVarAdjustRelationPersistence,
    RangeVarGetCreationNamespace, ResetTempTableNamespace,
};

pub use lookup::{
    get_collation_oid, get_namespace_oid, DeconstructQualifiedName, FuncCandidate, FuncnameGetCandidates,
    LookupExplicitNamespace, LookupNamespaceNoError, OperCandidate, OpernameGetCandidates,
    OpernameGetOprid, RangeVarGetRelid, RangeVarGetRelidExtended, RelnameGetRelid,
    TypenameGetTypidExtended, RVR_MISSING_OK, RVR_NOWAIT, RVR_SKIP_LOCKED,
};
pub use path::{
    assign_search_path, check_search_path, fetch_search_path, fetch_search_path_array,
    CopySearchPathMatcher, GetSearchPathMatcher, InitializeSearchPath, SearchPathMatcher,
    SearchPathMatchesCurrentEnvironment,
};
pub use visibility::{
    FunctionIsVisible, FunctionIsVisibleExt, RelationIsVisible, RelationIsVisibleExt,
    TypeIsVisible, TypeIsVisibleExt,
};

pub(crate) fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

struct PathState {
    mcx: Mcx<'static>,
    base_search_path: PgVec<'static, Oid>,
}

thread_local! {
    pub(crate) static MY_TEMP_NAMESPACE: Cell<Oid> = const { Cell::new(InvalidOid) };
    static MY_TEMP_TOAST_NAMESPACE: Cell<Oid> = const { Cell::new(InvalidOid) };
    static MY_TEMP_NAMESPACE_SUB_ID: Cell<SubTransactionId> =
        const { Cell::new(InvalidSubTransactionId) };
    pub(crate) static BASE_SEARCH_PATH_VALID: Cell<bool> = const { Cell::new(true) };
    pub(crate) static NAMESPACE_USER: Cell<Oid> = const { Cell::new(InvalidOid) };
    pub(crate) static BASE_CREATION_NAMESPACE: Cell<Oid> = const { Cell::new(InvalidOid) };
    pub(crate) static BASE_TEMP_CREATION_PENDING: Cell<bool> = const { Cell::new(false) };
    // INVARIANT: never zero (zero means "not known equal" in matchers).
    pub(crate) static ACTIVE_PATH_GENERATION: Cell<u64> = const { Cell::new(1) };
    pub(crate) static NAMESPACE_SEARCH_PATH: RefCell<Option<String>> = const { RefCell::new(None) };
    // The baseSearchPath list (C: TopMemoryContext). The context is leaked,
    // list replacement frees the old allocation through it.
    static PATH: RefCell<Option<ManuallyDrop<PathState>>> = const { RefCell::new(None) };
}

pub(crate) fn with_path_state<R>(f: impl FnOnce(&mut PathState) -> R) -> R {
    PATH.with(|cell| {
        let mut slot = cell.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            let mcx = Box::leak(Box::new(MemoryContext::new("namespace base search path"))).mcx();
            ManuallyDrop::new(PathState { mcx, base_search_path: PgVec::new_in(mcx) })
        });
        f(st)
    })
}

pub(crate) fn base_path_len() -> usize {
    with_path_state(|st| st.base_search_path.len())
}

pub(crate) fn base_path_nth(i: usize) -> Oid {
    with_path_state(|st| st.base_search_path[i])
}

pub(crate) fn my_temp_namespace() -> Oid {
    MY_TEMP_NAMESPACE.with(Cell::get)
}

fn my_temp_toast_namespace() -> Oid {
    MY_TEMP_TOAST_NAMESPACE.with(Cell::get)
}

pub fn isTempNamespace(namespaceId: Oid) -> bool {
    let mtn = my_temp_namespace();
    OidIsValid(mtn) && mtn == namespaceId
}

pub fn isTempToastNamespace(namespaceId: Oid) -> bool {
    let mttn = my_temp_toast_namespace();
    OidIsValid(mttn) && mttn == namespaceId
}

pub fn isTempOrTempToastNamespace(namespaceId: Oid) -> bool {
    let mtn = my_temp_namespace();
    OidIsValid(mtn) && (mtn == namespaceId || my_temp_toast_namespace() == namespaceId)
}

pub fn isAnyTempNamespace(namespaceId: Oid) -> PgResult<bool> {
    let Some(nspname) = syscache_seams::pg_namespace_nspname::call(namespaceId)? else {
        return Ok(false);
    };
    let name = nspname.name_str();
    Ok(name.starts_with(b"pg_temp_") || name.starts_with(b"pg_toast_temp_"))
}

pub fn isOtherTempNamespace(namespaceId: Oid) -> PgResult<bool> {
    if isTempOrTempToastNamespace(namespaceId) {
        return Ok(false);
    }
    isAnyTempNamespace(namespaceId)
}

pub fn GetTempNamespaceProcNumber(namespaceId: Oid) -> PgResult<ProcNumber> {
    let Some(nspname) = syscache_seams::pg_namespace_nspname::call(namespaceId)? else {
        return Ok(INVALID_PROC_NUMBER);
    };
    let name = nspname.name_str();
    Ok(if let Some(rest) = name.strip_prefix(b"pg_temp_") {
        atoi(rest)
    } else if let Some(rest) = name.strip_prefix(b"pg_toast_temp_") {
        atoi(rest)
    } else {
        INVALID_PROC_NUMBER
    })
}

// C atoi: optional whitespace/sign, then the leading digit run, else 0.
fn atoi(s: &[u8]) -> i32 {
    let mut i = 0;
    while i < s.len() && s[i].is_ascii_whitespace() {
        i += 1;
    }
    let mut sign: i64 = 1;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        if s[i] == b'-' {
            sign = -1;
        }
        i += 1;
    }
    let mut acc: i64 = 0;
    while i < s.len() && s[i].is_ascii_digit() {
        acc = acc.wrapping_mul(10).wrapping_add((s[i] - b'0') as i64);
        i += 1;
    }
    (sign * acc) as i32
}

pub fn GetTempToastNamespace() -> Oid {
    let mttn = my_temp_toast_namespace();
    debug_assert!(OidIsValid(mttn));
    mttn
}

pub fn GetTempNamespaceState() -> (Oid, Oid) {
    (my_temp_namespace(), my_temp_toast_namespace())
}

pub fn SetTempNamespaceState(tempNamespaceId: Oid, tempToastNamespaceId: Oid) {
    debug_assert_eq!(my_temp_namespace(), InvalidOid);
    debug_assert_eq!(my_temp_toast_namespace(), InvalidOid);
    debug_assert_eq!(MY_TEMP_NAMESPACE_SUB_ID.with(Cell::get), InvalidSubTransactionId);

    MY_TEMP_NAMESPACE.with(|c| c.set(tempNamespaceId));
    MY_TEMP_TOAST_NAMESPACE.with(|c| c.set(tempToastNamespaceId));

    BASE_SEARCH_PATH_VALID.with(|c| c.set(false));
    path::invalidate_search_path_cache();
}

pub fn AtEOXact_Namespace(isCommit: bool, parallel: bool) {
    if MY_TEMP_NAMESPACE_SUB_ID.with(Cell::get) != InvalidSubTransactionId && !parallel {
        if isCommit {
            ipc::before_shmem_exit(temp::RemoveTempRelationsCallback, datum::Datum::null())
                .expect("before_shmem_exit");
        } else {
            MY_TEMP_NAMESPACE.with(|c| c.set(InvalidOid));
            MY_TEMP_TOAST_NAMESPACE.with(|c| c.set(InvalidOid));
            BASE_SEARCH_PATH_VALID.with(|c| c.set(false));
            path::invalidate_search_path_cache();
        }
        MY_TEMP_NAMESPACE_SUB_ID.with(|c| c.set(InvalidSubTransactionId));
    }
}

pub fn AtEOSubXact_Namespace(
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) {
    if MY_TEMP_NAMESPACE_SUB_ID.with(Cell::get) == mySubid {
        if isCommit {
            MY_TEMP_NAMESPACE_SUB_ID.with(|c| c.set(parentSubid));
        } else {
            MY_TEMP_NAMESPACE_SUB_ID.with(|c| c.set(InvalidSubTransactionId));
            MY_TEMP_NAMESPACE.with(|c| c.set(InvalidOid));
            MY_TEMP_TOAST_NAMESPACE.with(|c| c.set(InvalidOid));
            BASE_SEARCH_PATH_VALID.with(|c| c.set(false));
            path::invalidate_search_path_cache();
        }
    }
}

fn seam_range_var_get_relid(
    _mcx: Mcx<'_>,
    relation: &rel_vocab::RangeVar<'_>,
    lockmode: types_rel::LOCKMODE,
    missing_ok: bool,
) -> PgResult<Oid> {
    let flags = if missing_ok { RVR_MISSING_OK } else { 0 };
    RangeVarGetRelidExtended(relation, lockmode, flags, None)
}

fn namespace_search_path_get() -> Option<String> {
    NAMESPACE_SEARCH_PATH.with(|s| s.borrow().clone())
}

fn namespace_search_path_set(value: Option<String>) {
    NAMESPACE_SEARCH_PATH.with(|s| *s.borrow_mut() = value);
}

pub fn init_seams() {
    namespace_seams::range_var_get_relid::set(seam_range_var_get_relid);
    namespace_seams::at_eoxact_namespace::set(AtEOXact_Namespace);
    namespace_seams::at_eosubxact_namespace::set(AtEOSubXact_Namespace);
    namespace_seams::is_temp_toast_namespace::set(isTempToastNamespace);
    namespace_seams::is_temp_namespace::set(isTempNamespace);
    namespace_seams::is_temp_or_temp_toast_namespace::set(isTempOrTempToastNamespace);
    namespace_seams::get_temp_namespace_proc_number::set(GetTempNamespaceProcNumber);
    namespace_seams::initialize_search_path::set(InitializeSearchPath);
    namespace_seams::fetch_search_path::set(fetch_search_path);
    namespace_seams::find_default_conversion_proc::set(lookup::FindDefaultConversionProc);
    namespace_seams::type_is_visible::set(lookup::TypeIsVisible);

    guc_tables::vars::namespace_search_path.install(guc_tables::GucVarAccessors {
        get: namespace_search_path_get,
        set: namespace_search_path_set,
    });
    guc_tables::hooks::check_search_path.install(path::check_search_path_hook);
    guc_tables::hooks::assign_search_path.install(path::assign_search_path_hook);
}
