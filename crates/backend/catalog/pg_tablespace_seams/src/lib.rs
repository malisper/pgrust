//! Seam declarations for the `pg_tablespace` catalog primitives that
//! `commands/tablespace.c` performs (`tablespace.c`'s `table_open`,
//! `table_beginscan_catalog` + `heap_getnext` over `pg_tablespace`, the
//! `heap_form_tuple`/`CatalogTupleInsert`, `heap_modify_tuple` +
//! `CatalogTupleUpdate`, and `CatalogTupleDelete` legs, plus the
//! `transformRelOptions` + `tablespace_reloptions` text-array build).
//!
//! There is no separate `pg_tablespace.c` translation unit in PostgreSQL;
//! these are the catalog operations inlined into tablespace.c. They are
//! homed here, behind seams, exactly as `seclabel.c`'s `pg_seclabel`
//! primitives are: the `Form_pg_tablespace` marshaling stays at the seam
//! boundary so the command crate carries no raw heap scan. The owner
//! (a future `pg_tablespace` catalog provider) installs these; until then
//! a call panics loudly.

#![allow(non_snake_case)]

use mcx::Mcx;
use seam_core::seam;
use types_core::primitive::Oid;
use types_error::PgResult;
use ::nodes::ddlnodes::DefElem;
use rel::Relation;
use types_storage::lock::LOCKMODE;
use types_tuple::heaptuple::ItemPointerData;

/// A `pg_tablespace` row matched by a catalog scan: the `spcoid` read out of
/// `((Form_pg_tablespace) GETSTRUCT(tuple))->oid` and the `tuple->t_self`
/// row identity used for a subsequent `CatalogTupleUpdate`/`CatalogTupleDelete`
/// (`heap_copytuple` keeps `t_self` valid past `table_endscan`).
#[derive(Clone, Copy, Debug)]
pub struct TablespaceTuple {
    pub oid: Oid,
    pub handle: ItemPointerData,
}

seam!(
    /// `table_open(TableSpaceRelationId, lockmode)` (access/table.h).
    pub fn tablespace_table_open<'mcx>(
        mcx: Mcx<'mcx>,
        lockmode: LOCKMODE,
    ) -> PgResult<Relation<'mcx>>
);

seam!(
    /// `table_close(rel, lockmode)` (access/table.h).
    pub fn tablespace_table_close<'mcx>(rel: Relation<'mcx>, lockmode: LOCKMODE) -> PgResult<()>
);

seam!(
    /// `table_beginscan_catalog` over `Anum_pg_tablespace_spcname == name`
    /// (`F_NAMEEQ`) + `heap_getnext` (at most one match), returning the row's
    /// oid + identity or `None`.
    pub fn scan_tablespace_by_name<'mcx>(
        rel: &Relation<'mcx>,
        name: &str,
    ) -> PgResult<Option<TablespaceTuple>>
);

seam!(
    /// `table_beginscan_catalog(rel, 0, NULL)` + `heap_getnext` loop over the
    /// whole `pg_tablespace` relation (`dbcommands.c`'s
    /// `CreateDatabaseUsingFileCopy` / `remove_dbtablespaces` /
    /// `check_db_file_conflict` each iterate every tablespace), returning the
    /// `((Form_pg_tablespace) GETSTRUCT(tuple))->oid` of every row in scan
    /// order. The caller has opened `rel` (`AccessShareLock`) and filters out
    /// `GLOBALTABLESPACE_OID` / does its own `GetDatabasePath` + `stat`/`rmtree`
    /// per oid, so only the bare oid list crosses.
    pub fn scan_all_tablespace_oids<'mcx>(
        rel: &Relation<'mcx>,
    ) -> PgResult<Vec<Oid>>
);

seam!(
    /// `table_beginscan_catalog` over `Anum_pg_tablespace_oid == spc_oid`
    /// (`F_OIDEQ`) + `heap_getnext`, returning the `pstrdup(NameStr(spcname))`
    /// of the match (palloc'd in `mcx`) or `None`.
    pub fn scan_tablespace_name_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        spc_oid: Oid,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
);

seam!(
    /// `transformRelOptions((Datum) 0, options, …, false, false)` +
    /// `(void) tablespace_reloptions(newOptions, true)` validation. Returns the
    /// serialized `spcoptions` text-array image (`None` ⇒ `nulls[spcoptions]`),
    /// validating the options as a side effect (can `ereport(ERROR)`).
    pub fn build_create_options<'mcx>(
        mcx: Mcx<'mcx>,
        options: &[DefElem<'mcx>],
    ) -> PgResult<Option<Vec<u8>>>
);

seam!(
    /// `heap_form_tuple(rel->rd_att, values, nulls)` for the new row
    /// (`oid`/`spcname`/`spcowner`, `spcacl` NULL, `spcoptions` from
    /// `new_options`) + `CatalogTupleInsert(rel, tuple)` + `heap_freetuple`.
    pub fn insert_tablespace_tuple<'mcx>(
        rel: &Relation<'mcx>,
        tablespaceoid: Oid,
        tablespacename: &str,
        owner_id: Oid,
        new_options: Option<Vec<u8>>,
    ) -> PgResult<()>
);

seam!(
    /// `heap_copytuple` + `namestrcpy(&newform->spcname, newname)` +
    /// `CatalogTupleUpdate(rel, &newtuple->t_self, newtuple)`.
    pub fn update_tablespace_name<'mcx>(
        rel: &Relation<'mcx>,
        handle: ItemPointerData,
        newname: &str,
    ) -> PgResult<()>
);

seam!(
    /// `AlterTableSpaceOptions`' options-update leg: read the existing
    /// `spcoptions` (`heap_getattr`), `transformRelOptions(old, options, …,
    /// is_reset)`, validate via `tablespace_reloptions`, then
    /// `heap_modify_tuple` (replacing `spcoptions`) + `CatalogTupleUpdate` +
    /// `heap_freetuple`.
    pub fn update_tablespace_options<'mcx, 'a>(
        rel: &Relation<'a>,
        handle: ItemPointerData,
        options: &[DefElem<'a>],
        is_reset: bool,
    ) -> PgResult<()>
);

seam!(
    /// `CatalogTupleDelete(rel, &tuple->t_self)` (indexing.c).
    pub fn delete_tablespace_tuple<'mcx>(
        rel: &Relation<'mcx>,
        handle: ItemPointerData,
    ) -> PgResult<()>
);
