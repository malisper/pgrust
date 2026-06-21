//! Seam declarations for the `backend-commands-tsearchcmds` unit
//! (`commands/tsearchcmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;

use mcx::{Mcx, PgVec};
use types_cache::DefElemString;
use backend_commands_define_seams::DefElemArg;
use types_core::Oid;
use types_error::PgResult;

/* ===========================================================================
 * Catalog-row snapshot carriers.
 *
 * The C threads opened `Relation`/`HeapTuple` handles between the catalog
 * insert, the `GETSTRUCT` form read, and `makeXxxDependencies`. The owned model
 * does not model those handles: each catalog seam is self-contained and returns
 * an owned snapshot of the row's relevant columns (`TS*Form`) or the new-row
 * field values (`NewTS*`), and the per-map rows cross as `ConfigMapEntry`. A
 * parser's `lextype` method result crosses as a `Vec<LexDescr>`.
 * ========================================================================= */

/// Typed snapshot of a `pg_ts_parser` row's OID columns (for
/// `makeParserDependencies`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TSParserForm {
    pub oid: Oid,
    pub prsnamespace: Oid,
    pub prsstart: Oid,
    pub prstoken: Oid,
    pub prsend: Oid,
    pub prsheadline: Oid,
    pub prslextype: Oid,
}

/// Typed snapshot of a `pg_ts_dict` row (for `makeDictionaryDependencies`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TSDictForm {
    pub oid: Oid,
    pub dictnamespace: Oid,
    pub dictowner: Oid,
    pub dicttemplate: Oid,
}

/// Typed snapshot of a `pg_ts_template` row (for `makeTSTemplateDependencies`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TSTemplateForm {
    pub oid: Oid,
    pub tmplnamespace: Oid,
    pub tmplinit: Oid,
    pub tmpllexize: Oid,
}

/// Typed snapshot of a `pg_ts_config` row (for `makeConfigurationDependencies`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TSConfigForm {
    pub oid: Oid,
    pub cfgnamespace: Oid,
    pub cfgowner: Oid,
    pub cfgparser: Oid,
}

/// Field values used to build a new `pg_ts_parser` row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NewTSParser {
    pub prsname: alloc::string::String,
    pub prsnamespace: Oid,
    pub prsstart: Oid,
    pub prstoken: Oid,
    pub prsend: Oid,
    pub prsheadline: Oid,
    pub prslextype: Oid,
}

/// Field values used to build a new `pg_ts_template` row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NewTSTemplate {
    pub tmplname: alloc::string::String,
    pub tmplnamespace: Oid,
    pub tmplinit: Oid,
    pub tmpllexize: Oid,
}

/// A `(maptokentype, mapseqno, mapdict)` triple of a `pg_ts_config_map` row.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConfigMapEntry {
    pub maptokentype: i32,
    pub mapseqno: i32,
    pub mapdict: Oid,
}

/// A `LexDescr` row from a parser's `lextype` method: `(lexid, alias)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LexDescr {
    pub lexid: i32,
    pub alias: alloc::string::String,
}

seam_core::seam!(
    /// `deserialize_deflist(txt)` (tsearchcmds.c): build a `List` of
    /// `DefElem` from a stored `text` datum. `txt` is the verbatim varlena
    /// bytes a `SysCacheGetAttr` read produced (including the header,
    /// possibly compressed); the owner performs the C `TextDatumGetCString`
    /// detoast + conversion. Every produced `DefElem` has a `String`-node
    /// argument, so the list crosses as typed rows allocated in `mcx`. `Err`
    /// carries the C `ereport(ERROR, "invalid deserialize_deflist syntax")`
    /// and OOM.
    pub fn deserialize_deflist<'mcx>(
        mcx: Mcx<'mcx>,
        txt: &[u8],
    ) -> PgResult<PgVec<'mcx, DefElemString<'mcx>>>
);

seam_core::seam!(
    /// `RemoveTSConfigurationById(cfgId)` (commands/tsearchcmds.c): the
    /// per-class `OCLASS_TSCONFIG` drop handler dependency.c's `doDeletion`
    /// invokes for a `pg_ts_config` object. Removes the text-search
    /// configuration's catalog rows. Can `ereport(ERROR)`, carried on `Err`.
    pub fn RemoveTSConfigurationById(cfgId: Oid) -> PgResult<()>
);

/* ===========================================================================
 * Outward seams: catalog write engine.
 *
 * Each `insert_ts_*` opens the relation, allocates the OID
 * (`GetNewOidWithIndex`), `heap_form_tuple`s the row, `CatalogTupleInsert`s it,
 * and returns `(newOid, form-snapshot)`. The catalog write engine owns these;
 * they panic until it lands.
 * ========================================================================= */

seam_core::seam!(
    /// `CREATE TEXT SEARCH PARSER` catalog insert (`DefineTSParser`): build the
    /// `pg_ts_parser` row from `row`, `GetNewOidWithIndex` its OID, and
    /// `CatalogTupleInsert`. Returns the new OID and the row form snapshot.
    pub fn insert_ts_parser(row: &NewTSParser) -> PgResult<(Oid, TSParserForm)>
);

seam_core::seam!(
    /// `CREATE TEXT SEARCH TEMPLATE` catalog insert (`DefineTSTemplate`).
    pub fn insert_ts_template(row: &NewTSTemplate) -> PgResult<(Oid, TSTemplateForm)>
);

seam_core::seam!(
    /// `CREATE TEXT SEARCH DICTIONARY` catalog insert (`DefineTSDictionary`):
    /// build + insert the `pg_ts_dict` row. `dictoptions` is the serialized
    /// option text (`None` => `dictinitoption` SQL NULL).
    pub fn insert_ts_dict(
        name: &str,
        namespaceoid: Oid,
        owner: Oid,
        templ_id: Oid,
        dictoptions: Option<&str>,
    ) -> PgResult<(Oid, TSDictForm)>
);

seam_core::seam!(
    /// `CREATE TEXT SEARCH CONFIGURATION` catalog insert
    /// (`DefineTSConfiguration`): build + insert the `pg_ts_config` row.
    pub fn insert_ts_config(
        name: &str,
        namespaceoid: Oid,
        owner: Oid,
        prs_oid: Oid,
    ) -> PgResult<(Oid, TSConfigForm)>
);

seam_core::seam!(
    /// `SearchSysCache1(TSCONFIGOID, sourceOid)` + `GETSTRUCT`
    /// (`DefineTSConfiguration` COPY path) — the source config's form snapshot.
    pub fn config_form_by_oid(source_oid: Oid) -> PgResult<TSConfigForm>
);

seam_core::seam!(
    /// `GetTSConfigTuple(names)` (tsearchcmds.c): `get_ts_config_oid(.., true)`
    /// then `SearchSysCache1(TSCONFIGOID, ..)`; `None` when no such config.
    pub fn get_ts_config_form(names: &[Option<String>]) -> PgResult<Option<TSConfigForm>>
);

seam_core::seam!(
    /// The `pg_ts_config_map` rows for `cfg_id`
    /// (`systable_beginscan(.., mapcfg = cfg_id)`), as `(token, seqno, dict)`
    /// triples. Used by the COPY map-copy and `makeConfigurationDependencies`.
    pub fn config_map_entries<'mcx>(
        mcx: Mcx<'mcx>,
        cfg_id: Oid,
    ) -> PgResult<PgVec<'mcx, ConfigMapEntry>>
);

seam_core::seam!(
    /// `CatalogTuplesMultiInsertWithInfo` of new `pg_ts_config_map` rows for
    /// `cfg_id` (the COPY map-copy and the `MakeConfigurationMapping` insertion
    /// path). The `maptokentype`/`mapseqno`/`mapdict` come from `entries`.
    pub fn insert_config_map_entries(cfg_id: Oid, entries: &[ConfigMapEntry]) -> PgResult<()>
);

seam_core::seam!(
    /// `MakeConfigurationMapping` REPLACE path: for `pg_ts_config_map` rows of
    /// `cfg_id` whose `maptokentype` is in `token_nums` (or all rows when
    /// `token_nums` is empty) and whose `mapdict == dict_old`,
    /// `heap_modify_tuple` `mapdict` to `dict_new` + `CatalogTupleUpdateWithInfo`.
    pub fn replace_config_map_dict(
        cfg_id: Oid,
        token_nums: &[i32],
        dict_old: Oid,
        dict_new: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `MakeConfigurationMapping`/`DropConfigurationMapping` per-token delete:
    /// `CatalogTupleDelete` every `pg_ts_config_map` row with
    /// `(mapcfg = cfg_id, maptokentype = token_num)`; returns the count deleted
    /// (`DropConfigurationMapping`'s `found` flag is `count > 0`).
    pub fn delete_config_map_for_token(cfg_id: Oid, token_num: i32) -> PgResult<i64>
);

seam_core::seam!(
    /// `RemoveTSConfigurationById` map-clearing scan: delete every
    /// `pg_ts_config_map` row with `mapcfg = cfg_id`.
    pub fn delete_config_map_for_cfg(cfg_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `RemoveTSConfigurationById` row delete:
    /// `SearchSysCache1(TSCONFIGOID, cfg_id)` + `CatalogTupleDelete` the
    /// `pg_ts_config` row.
    pub fn delete_ts_config_row(cfg_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterTSDictionary` syscache read: `SearchSysCache1(TSDICTOID, dict_id)`,
    /// then the dict's `dicttemplate` OID + the raw `dictinitoption` text
    /// (`None` when the attribute is SQL NULL). Built as the verbatim deserialized
    /// option string the owner produced via `TextDatumGetCString`.
    pub fn dict_options_and_template(
        dict_id: Oid,
    ) -> PgResult<(Oid, Option<alloc::string::String>)>
);

seam_core::seam!(
    /// `AlterTSDictionary` update: `heap_modify_tuple` the `pg_ts_dict` row's
    /// `dictinitoption` to `opttext` (`None` => SQL NULL) + `CatalogTupleUpdate`.
    pub fn update_dict_options(dict_id: Oid, opttext: Option<&str>) -> PgResult<()>
);

seam_core::seam!(
    /// `verify_dictoptions` template read: `SearchSysCache1(TSTEMPLATEOID,
    /// tmpl_id)`, then `(tmplname, tmplinit)`. `None` when no such template
    /// (the C "cache lookup failed" `elog`).
    pub fn ts_template_init_method(
        tmpl_id: Oid,
    ) -> PgResult<Option<(alloc::string::String, Oid)>>
);

seam_core::seam!(
    /// `getTokenTypes` parser-cache read: `lookup_ts_parser_cache(prs_id)`'s
    /// `lextypeOid` (InvalidOid when the parser has no lextype method).
    pub fn parser_lextype_oid(prs_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `getTokenTypes` lextype dispatch: `OidFunctionCall1(lextypeOid, 0)`,
    /// returning the `LexDescr[]` the parser's lextype method yields (terminated
    /// by a `lexid == 0` entry in C; the owner returns the full array).
    pub fn call_parser_lextype<'mcx>(
        mcx: Mcx<'mcx>,
        lextype_oid: Oid,
    ) -> PgResult<PgVec<'mcx, LexDescr>>
);

seam_core::seam!(
    /// `verify_dictoptions` init dispatch:
    /// `OidFunctionCall1(initmethod, PointerGetDatum(dictoptions))` — call the
    /// template's init method on the option list; the call's only purpose is to
    /// let the init method validate the options and complain via
    /// `ereport(ERROR)`. Each option crosses as `(defname, arg)` where `arg`
    /// preserves the `DefElem`'s node kind (`T_Integer`/`T_Float`/`T_Boolean`/
    /// `T_String`/...), because init methods read it with `defGetBoolean` etc.,
    /// which switch on the node tag (e.g. `casesensitive = 1` is a `T_Integer`).
    pub fn call_dict_init(
        initmethod: Oid,
        dictoptions: &[(String, Option<DefElemArg>)],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `EventTriggerCollectAlterTSConfig(stmt, cfgId, dictIds, ndict)`
    /// (event_trigger.c): collect the ALTER TEXT SEARCH CONFIGURATION command
    /// for an event trigger. The `dict_ids` slice is the C `Oid *dictIds`
    /// (empty for the DROP-mapping path's `NULL, 0`).
    pub fn event_trigger_collect_alter_ts_config(cfg_id: Oid, dict_ids: &[Oid]) -> PgResult<()>
);
