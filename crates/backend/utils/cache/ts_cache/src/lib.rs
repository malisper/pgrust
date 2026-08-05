#![allow(non_snake_case, non_upper_case_globals)]

mod deflist;
#[cfg(test)]
mod tests;

pub use self::deflist::{deserialize_deflist, DefListItem};

use core::cell::{Cell, RefCell};
use core::mem::ManuallyDrop;
use std::rc::Rc;

use self::cache_ids::{TSCONFIGMAP, TSCONFIGOID, TSDICTOID, TSPARSEROID, TSTEMPLATEOID};
use datum::Datum;
use mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgHashMap, PgVec};
use ts_locale::dict_api::DictInitData;
use ts_locale::DictSubState;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_UNDEFINED_OBJECT};
use types_fmgr::{
    function_call1_coll_in, function_call4_coll_in, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo,
};

// Syscache ids for callback registration (cacheinfo.rs order).
mod cache_ids {
    pub const TSCONFIGMAP: i32 = 72;
    pub const TSCONFIGOID: i32 = 74;
    pub const TSDICTOID: i32 = 76;
    pub const TSPARSEROID: i32 = 78;
    pub const TSTEMPLATEOID: i32 = 80;
}

pub const MAXTOKENTYPE: usize = 256;
pub const MAXDICTSPERTT: usize = 100;

pub struct TSParserCacheEntry {
    pub prs_id: Oid,
    pub isvalid: Cell<bool>,
    pub start_oid: Oid,
    pub token_oid: Oid,
    pub end_oid: Oid,
    pub headline_oid: Oid,
    pub lextype_oid: Oid,
    pub prsstart: RefCell<FmgrInfo>,
    pub prstoken: RefCell<FmgrInfo>,
    pub prsend: RefCell<FmgrInfo>,
    pub prsheadline: RefCell<Option<FmgrInfo>>,
}

pub struct TSDictionaryCacheEntry {
    pub dict_id: Oid,
    pub isvalid: Cell<bool>,
    pub lexize_oid: Oid,
    // Owns dict_data and everything the init method allocated.
    // Heap-pinned: Mcx handles into it live inside dict_data (PgVec allocators).
    _dict_ctx: Option<std::boxed::Box<MemoryContext>>,
    pub dict_data: usize,
    lexize: RefCell<FmgrInfo>,
}

impl TSDictionaryCacheEntry {
    // FunctionCall4(&entry->lexize, dictData, VARDATA(in), len, &dstate); the
    // returned word is a *mut LexizeResult in `result_mcx` or 0 (C NULL).
    pub fn call_lexize(
        &self,
        result_mcx: Mcx<'_>,
        token: &[u8],
        dstate: Option<&mut DictSubState>,
    ) -> PgResult<usize> {
        let dstate_word = match dstate {
            Some(s) => s as *mut DictSubState as usize,
            None => 0,
        };
        let mut lexize = self.lexize.borrow_mut();
        let d = function_call4_coll_in(
            &mut lexize,
            InvalidOid,
            result_mcx,
            Datum::from_usize(self.dict_data),
            Datum::from_usize(token.as_ptr() as usize),
            Datum::from_i32(token.len() as i32),
            Datum::from_usize(dstate_word),
        )?;
        Ok(d.as_usize())
    }
}

pub struct ListDictionary {
    pub dict_ids: PgVec<'static, Oid>,
}

pub struct TSConfigCacheEntry {
    pub cfg_id: Oid,
    pub isvalid: Cell<bool>,
    pub prs_id: Oid,
    // Indexed by token type; empty dict_ids = no mapping for that type.
    pub map: PgVec<'static, ListDictionary>,
}

struct TsCacheState {
    mcx: Mcx<'static>,
    parsers: PgHashMap<'static, Oid, Rc<TSParserCacheEntry>>,
    dicts: PgHashMap<'static, Oid, Rc<TSDictionaryCacheEntry>>,
    configs: PgHashMap<'static, Oid, Rc<TSConfigCacheEntry>>,
    last_parser: Option<Rc<TSParserCacheEntry>>,
    last_dict: Option<Rc<TSDictionaryCacheEntry>>,
    last_config: Option<Rc<TSConfigCacheEntry>>,
    parser_cb_registered: bool,
    dict_cb_registered: bool,
    config_cb_registered: bool,
    current_config: Oid,
}

thread_local! {
    static STATE: RefCell<Option<ManuallyDrop<TsCacheState>>> = const { RefCell::new(None) };
    static GUC_VALUE: RefCell<Option<String>> = const { RefCell::new(None) };
}

fn with_state<R>(f: impl FnOnce(&mut TsCacheState) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            let mcx = ::mcx::session_root("TsCacheContext").mcx();
            // LIFO: drop the state properly before the context free (any
            // global-heap entry contents are released by the drop glue).
            ::mcx::register_session_cleanup(Box::new(|| {
                STATE.with(|cell| {
                    if let Some(st) = cell.borrow_mut().take() {
                        drop(ManuallyDrop::into_inner(st));
                    }
                });
            }));
            ManuallyDrop::new(TsCacheState {
                mcx,
                parsers: PgHashMap::with_capacity_in(4, mcx),
                dicts: PgHashMap::with_capacity_in(8, mcx),
                configs: PgHashMap::with_capacity_in(16, mcx),
                last_parser: None,
                last_dict: None,
                last_config: None,
                parser_cb_registered: false,
                dict_cb_registered: false,
                config_cb_registered: false,
                current_config: InvalidOid,
            })
        });
        f(st)
    })
}

fn InvalidateParserCallBack(_arg: Datum, _cacheid: i32, _hash: u32) {
    with_state(|st| {
        for e in st.parsers.values() {
            e.isvalid.set(false);
        }
    });
}

fn InvalidateDictCallBack(_arg: Datum, _cacheid: i32, _hash: u32) {
    with_state(|st| {
        for e in st.dicts.values() {
            e.isvalid.set(false);
        }
    });
}

fn InvalidateConfigCallBack(_arg: Datum, _cacheid: i32, _hash: u32) {
    with_state(|st| {
        for e in st.configs.values() {
            e.isvalid.set(false);
        }
        st.current_config = InvalidOid;
    });
}

#[track_caller]
#[cold]
fn cache_lookup_failed(kind: &str, oid: Oid) -> Box<PgError> {
    PgError::error(format!("cache lookup failed for text search {kind} {oid}")).into()
}

pub fn lookup_ts_parser_cache(prsId: Oid) -> PgResult<Rc<TSParserCacheEntry>> {
    let registered = with_state(|st| st.parser_cb_registered);
    if !registered {
        inval::invalidate::CacheRegisterSyscacheCallback(
            TSPARSEROID,
            InvalidateParserCallBack,
            Datum::null(),
        )?;
        with_state(|st| st.parser_cb_registered = true);
    }

    if let Some(hit) = with_state(|st| {
        if let Some(last) = &st.last_parser {
            if last.prs_id == prsId && last.isvalid.get() {
                return Some(Rc::clone(last));
            }
        }
        st.parsers.get(&prsId).filter(|e| e.isvalid.get()).map(Rc::clone)
    }) {
        with_state(|st| st.last_parser = Some(Rc::clone(&hit)));
        return Ok(hit);
    }

    let prs = syscache_seams::lookup_pg_ts_parser_shape::call(prsId)?
        .ok_or_else(|| cache_lookup_failed("parser", prsId))?;
    for (oid, method) in [
        (prs.prsstart, "prsstart"),
        (prs.prstoken, "prstoken"),
        (prs.prsend, "prsend"),
    ] {
        if oid == InvalidOid {
            return Err(PgError::error(format!(
                "text search parser {prsId} has no {method} method"
            ))
            .into());
        }
    }

    let entry = Rc::new(TSParserCacheEntry {
        prs_id: prsId,
        isvalid: Cell::new(true),
        start_oid: prs.prsstart,
        token_oid: prs.prstoken,
        end_oid: prs.prsend,
        headline_oid: prs.prsheadline,
        lextype_oid: prs.prslextype,
        prsstart: RefCell::new(fmgr_seams::fmgr_info::call(prs.prsstart)?),
        prstoken: RefCell::new(fmgr_seams::fmgr_info::call(prs.prstoken)?),
        prsend: RefCell::new(fmgr_seams::fmgr_info::call(prs.prsend)?),
        prsheadline: RefCell::new(if prs.prsheadline != InvalidOid {
            Some(fmgr_seams::fmgr_info::call(prs.prsheadline)?)
        } else {
            None
        }),
    });
    with_state(|st| {
        st.parsers.insert(prsId, Rc::clone(&entry));
        st.last_parser = Some(Rc::clone(&entry));
    });
    Ok(entry)
}

pub fn lookup_ts_dictionary_cache(dictId: Oid) -> PgResult<Rc<TSDictionaryCacheEntry>> {
    let registered = with_state(|st| st.dict_cb_registered);
    if !registered {
        inval::invalidate::CacheRegisterSyscacheCallback(
            TSDICTOID,
            InvalidateDictCallBack,
            Datum::null(),
        )?;
        inval::invalidate::CacheRegisterSyscacheCallback(
            TSTEMPLATEOID,
            InvalidateDictCallBack,
            Datum::null(),
        )?;
        with_state(|st| st.dict_cb_registered = true);
    }

    if let Some(hit) = with_state(|st| {
        if let Some(last) = &st.last_dict {
            if last.dict_id == dictId && last.isvalid.get() {
                return Some(Rc::clone(last));
            }
        }
        st.dicts.get(&dictId).filter(|e| e.isvalid.get()).map(Rc::clone)
    }) {
        with_state(|st| st.last_dict = Some(Rc::clone(&hit)));
        return Ok(hit);
    }

    let ctx = std::boxed::Box::new(MemoryContext::new("TS dictionary"));
    let (template_oid, init_oid, lexize_oid, dict_data);
    {
        // SAFETY: 'static stands for "as long as the Box in _dict_ctx lives";
        // the box pins the context address across the move into the entry.
        let dmcx: Mcx<'static> = unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>(ctx.mcx()) };
        let dict = syscache_seams::lookup_pg_ts_dict_shape::call(dmcx, dictId)?
            .ok_or_else(|| cache_lookup_failed("dictionary", dictId))?;
        template_oid = dict.dicttemplate;
        if template_oid == InvalidOid {
            return Err(PgError::error(format!(
                "text search dictionary {dictId} has no template"
            ))
            .into());
        }
        let tmpl = syscache_seams::lookup_pg_ts_template_shape::call(template_oid)?
            .ok_or_else(|| cache_lookup_failed("template", template_oid))?;
        init_oid = tmpl.tmplinit;
        lexize_oid = tmpl.tmpllexize;
        if lexize_oid == InvalidOid {
            return Err(PgError::error(format!(
                "text search template {template_oid} has no lexize method"
            ))
            .into());
        }

        dict_data = if init_oid != InvalidOid {
            let mut dict_options: PgVec<'_, (PgVec<'_, u8>, PgVec<'_, u8>)> = PgVec::new_in(dmcx);
            let mut int_options: PgVec<'_, Option<i64>> = PgVec::new_in(dmcx);
            if let Some(opt) = dict.dictinitoption {
                for item in deserialize_deflist(dmcx, &opt)? {
                    dict_options.push((item.name, item.value));
                    int_options.push(item.int_value);
                }
            }
            let init_data = DictInitData { mcx: dmcx, dict_options, int_options };
            let mut init_f = fmgr_seams::fmgr_info::call(init_oid)?;
            function_call1_coll_in(
                &mut init_f,
                InvalidOid,
                dmcx,
                Datum::from_usize(&init_data as *const DictInitData<'_> as usize),
            )?
            .as_usize()
        } else {
            0
        };
    }

    let entry = Rc::new(TSDictionaryCacheEntry {
        dict_id: dictId,
        isvalid: Cell::new(true),
        lexize_oid,
        _dict_ctx: if init_oid != InvalidOid { Some(ctx) } else { None },
        dict_data,
        lexize: RefCell::new(fmgr_seams::fmgr_info::call(lexize_oid)?),
    });
    with_state(|st| {
        st.dicts.insert(dictId, Rc::clone(&entry));
        st.last_dict = Some(Rc::clone(&entry));
    });
    Ok(entry)
}

fn ensure_config_callbacks() -> PgResult<()> {
    let registered = with_state(|st| st.config_cb_registered);
    if !registered {
        inval::invalidate::CacheRegisterSyscacheCallback(
            TSCONFIGOID,
            InvalidateConfigCallBack,
            Datum::null(),
        )?;
        inval::invalidate::CacheRegisterSyscacheCallback(
            TSCONFIGMAP,
            InvalidateConfigCallBack,
            Datum::null(),
        )?;
        with_state(|st| st.config_cb_registered = true);
    }
    Ok(())
}

pub fn lookup_ts_config_cache(cfgId: Oid) -> PgResult<Rc<TSConfigCacheEntry>> {
    ensure_config_callbacks()?;

    if let Some(hit) = with_state(|st| {
        if let Some(last) = &st.last_config {
            if last.cfg_id == cfgId && last.isvalid.get() {
                return Some(Rc::clone(last));
            }
        }
        st.configs.get(&cfgId).filter(|e| e.isvalid.get()).map(Rc::clone)
    }) {
        with_state(|st| st.last_config = Some(Rc::clone(&hit)));
        return Ok(hit);
    }

    let cfg = syscache_seams::lookup_pg_ts_config_shape::call(cfgId)?
        .ok_or_else(|| cache_lookup_failed("configuration", cfgId))?;
    let prs_id = cfg.cfgparser;
    if prs_id == InvalidOid {
        return Err(
            PgError::error(format!("text search configuration {cfgId} has no parser")).into()
        );
    }

    let state_mcx = with_state(|st| st.mcx);
    let scratch = MemoryContext::new("ts_config map scan");
    let rows = syscache_seams::pg_ts_config_map_shapes::call(scratch.mcx(), cfgId)?;

    let mut maxtokentype = 0usize;
    for r in rows.iter() {
        let toktype = r.maptokentype;
        if toktype <= 0 || toktype as usize > MAXTOKENTYPE {
            return Err(
                PgError::error(format!("maptokentype value {toktype} is out of range")).into()
            );
        }
        maxtokentype = toktype as usize;
    }
    let lenmap = if rows.is_empty() { 0 } else { maxtokentype + 1 };
    let mut map: PgVec<'static, ListDictionary> = PgVec::new_in(state_mcx);
    map.try_reserve_exact(lenmap).map_err(|_| state_mcx.oom(lenmap))?;
    for _ in 0..lenmap {
        map.push(ListDictionary { dict_ids: PgVec::new_in(state_mcx) });
    }
    for r in rows.iter() {
        let dicts = &mut map[r.maptokentype as usize].dict_ids;
        if dicts.len() >= MAXDICTSPERTT {
            return Err(
                PgError::error("too many pg_ts_config_map entries for one token type").into()
            );
        }
        dicts.push(r.mapdict);
    }

    let entry = Rc::new(TSConfigCacheEntry {
        cfg_id: cfgId,
        isvalid: Cell::new(true),
        prs_id,
        map,
    });
    with_state(|st| {
        st.configs.insert(cfgId, Rc::clone(&entry));
        st.last_config = Some(Rc::clone(&entry));
    });
    Ok(entry)
}

// DeconstructQualifiedName (namespace.c).
fn deconstruct_qualified_name<'a>(names: &[&'a str]) -> PgResult<(Option<&'a str>, &'a str)> {
    match names {
        [objname] => Ok((None, objname)),
        [schemaname, objname] => Ok((Some(schemaname), objname)),
        [catalogname, schemaname, objname] => {
            let dbname =
                dbcommands_seams::get_database_name::call(init_small::globals::MyDatabaseId())?;
            if dbname.as_deref() != Some(*catalogname) {
                return Err(PgError::error(format!(
                    "cross-database references are not implemented: {}",
                    names.join(".")
                ))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .into());
            }
            Ok((Some(schemaname), objname))
        }
        _ => Err(PgError::error(format!(
            "improper qualified name (too many dotted names): {}",
            names.join(".")
        ))
        .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR)
        .into()),
    }
}

// get_ts_config_oid / get_ts_dict_oid (C home: namespace.c) — hosted here
// until a namespace tsearch arm exists; the search-path walk skips the temp
// namespace like C.
fn ts_name_lookup(
    names: &[&str],
    missing_ok: bool,
    by_name: fn(&str, Oid) -> PgResult<Oid>,
    kind: &str,
) -> PgResult<Oid> {
    let (schemaname, name) = deconstruct_qualified_name(names)?;
    let mut result = InvalidOid;
    if let Some(schemaname) = schemaname {
        let namespace_id = namespace_seams::lookup_explicit_namespace::call(schemaname, missing_ok)?;
        if namespace_id != InvalidOid {
            result = by_name(name, namespace_id)?;
        }
    } else {
        let scratch = MemoryContext::new("ts_name_lookup");
        for namespace_id in namespace_seams::fetch_search_path::call(scratch.mcx(), true)?.iter() {
            if namespace_seams::is_temp_namespace::call(*namespace_id) {
                continue;
            }
            result = by_name(name, *namespace_id)?;
            if result != InvalidOid {
                break;
            }
        }
    }
    if result == InvalidOid && !missing_ok {
        return Err(PgError::error(format!(
            "text search {kind} \"{}\" does not exist",
            names.join(".")
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
        .into());
    }
    Ok(result)
}

pub fn get_ts_config_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    ts_name_lookup(
        names,
        missing_ok,
        |n, nsp| syscache_seams::lookup_pg_ts_config_oid_by_name::call(n, nsp),
        "configuration",
    )
}

pub fn get_ts_dict_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    ts_name_lookup(
        names,
        missing_ok,
        |n, nsp| syscache_seams::lookup_pg_ts_dict_oid_by_name::call(n, nsp),
        "dictionary",
    )
}

pub fn getTSCurrentConfig(emitError: bool) -> PgResult<Oid> {
    let cached = with_state(|st| st.current_config);
    if cached != InvalidOid {
        return Ok(cached);
    }
    let value = GUC_VALUE.with(|v| v.borrow().clone());
    let value = match value {
        Some(v) if !v.is_empty() => v,
        _ => {
            if emitError {
                return Err(PgError::error("text search configuration isn't set").into());
            }
            return Ok(InvalidOid);
        }
    };
    ensure_config_callbacks()?;

    let scratch = MemoryContext::new("getTSCurrentConfig");
    let names = if emitError {
        adt_regproc::string_to_qualified_name_list(scratch.mcx(), &value, None)?
    } else {
        let mut esc = types_error::SoftErrorContext::new(false);
        adt_regproc::string_to_qualified_name_list(scratch.mcx(), &value, Some(&mut esc))?
    };
    let oid = match names {
        Some(names) => {
            let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            get_ts_config_oid(&refs, !emitError)?
        }
        None => InvalidOid,
    };
    with_state(|st| st.current_config = oid);
    Ok(oid)
}

fn guc_get() -> Option<String> {
    GUC_VALUE.with(|v| v.borrow().clone())
}

fn guc_set(newval: Option<String>) {
    GUC_VALUE.with(|v| *v.borrow_mut() = newval);
}

fn check_default_text_search_config(
    newval: &mut Option<String>,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    source: types_guc::GucSource,
) -> PgResult<bool> {
    if !xact_seams::is_transaction_state::call()
        || init_small::globals::MyDatabaseId() == InvalidOid
    {
        return Ok(true);
    }
    let Some(val) = newval.as_deref() else {
        return Ok(true);
    };
    let scratch = MemoryContext::new("check_default_text_search_config");
    let mcx = scratch.mcx();

    let mut esc = types_error::SoftErrorContext::new(false);
    let names = adt_regproc::string_to_qualified_name_list(mcx, val, Some(&mut esc))?;
    let cfg_id = match names {
        Some(names) => {
            let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            get_ts_config_oid(&refs, true)?
        }
        None => InvalidOid,
    };
    if cfg_id == InvalidOid {
        if source == types_guc::GucSource::PGC_S_TEST {
            elog::elog(
                types_error::NOTICE,
                format!("text search configuration \"{val}\" does not exist"),
            )?;
            return Ok(true);
        }
        return Ok(false);
    }

    let cfg = syscache_seams::lookup_pg_ts_config_shape::call(cfg_id)?
        .ok_or_else(|| cache_lookup_failed("configuration", cfg_id))?;
    let nspname = lsyscache::misc::get_namespace_name(mcx, cfg.cfgnamespace)?
        .unwrap_or_else(|| panic!("cache lookup failed for namespace {}", cfg.cfgnamespace));
    let name_str = core::str::from_utf8(cfg.cfgname.name_str()).unwrap_or("");
    // quote_qualified_identifier minus quote_all_identifiers (format_type's
    // GUC-less variant; the GUC slot is uninstalled repo-wide).
    let qualified = format!(
        "{}.{}",
        format_type::quote_identifier(nspname.as_str()),
        format_type::quote_identifier(name_str),
    );
    *newval = Some(qualified);
    Ok(true)
}

fn assign_default_text_search_config(
    _newval: Option<&str>,
    _extra: Option<&guc_tables::GucHookExtra>,
) {
    with_state(|st| st.current_config = InvalidOid);
}

pub fn init_hooks() {
    guc_tables::vars::TSCurrentConfig
        .install(guc_tables::GucVarAccessors { get: guc_get, set: guc_set });
    guc_tables::hooks::check_default_text_search_config.install(check_default_text_search_config);
    guc_tables::hooks::assign_default_text_search_config
        .install(assign_default_text_search_config);
}

pub fn fc_get_current_ts_config(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_oid(getTSCurrentConfig(true)?))
}

pub mod builtins {
    use super::*;

    const fn b(
        foid: Oid,
        name: &'static str,
        nargs: i16,
        func: types_fmgr::PGFunction,
    ) -> FmgrBuiltin {
        FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
    }

    pub const TS_CACHE_BUILTINS: &[FmgrBuiltin] =
        &[b(3759, "get_current_ts_config", 0, fc_get_current_ts_config)];
}
