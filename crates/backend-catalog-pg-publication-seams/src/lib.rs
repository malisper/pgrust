//! Seam declarations for the `backend-catalog-pg-publication` unit
//! (`catalog/pg_publication.c`): the public entry points the publication
//! command / catalog / replication units call across a dependency cycle
//! (publicationcmds.c `CREATE`/`ALTER PUBLICATION`, the relcache publication-
//! desc rebuild, subscriptioncmds.c, and the `pgoutput` plugin).
//!
//! `backend-catalog-pg-publication` installs every one of these from its own
//! `init_seams()`. The allocating entry points take `Mcx<'mcx>` and carry the
//! caller's lifetime on their allocated outputs. There is no ambient context;
//! `mcx` is threaded explicitly.
//!
//! NOTE: the future consumers (the relcache `rd_pubdesc` builder owner,
//! publicationcmds, subscriptioncmds, pgoutput) are not yet built, so these
//! seams are installed-but-unconsumed for now. The owner crate installs them
//! all so they are live the moment a consumer lands.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_publication::{
    Publication, PublicationPartOpt, PublicationTableRow, PublishGencolsType,
};
use types_core::Oid;
use types_error::PgResult;
use mcx::PgBox;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::nodes::Node;
use types_rel::Relation;

/* ==========================================================================
 * Lookups returning a decoded `Publication`.
 * ========================================================================== */

seam_core::seam!(
    /// `GetPublication(pubid)` — decode the `pg_publication` row into an owned
    /// [`Publication`]. Errors (cache lookup failed) if no such publication.
    pub fn GetPublication<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<Publication<'mcx>>
);

seam_core::seam!(
    /// `GetPublicationByName(pubname, missing_ok)` — `None` when missing and
    /// `missing_ok`, else the decoded publication.
    pub fn GetPublicationByName<'mcx>(
        mcx: Mcx<'mcx>,
        pubname: &str,
        missing_ok: bool,
    ) -> PgResult<Option<Publication<'mcx>>>
);

/* ==========================================================================
 * Publication-oid / relation-oid / schema-oid list getters.
 * ========================================================================== */

seam_core::seam!(
    /// `GetRelationPublications(relid)` — publication oids that publish `relid`
    /// directly (via `pg_publication_rel`).
    pub fn GetRelationPublications<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `GetPublicationRelations(pubid, pub_partopt)` — relation oids in a FOR
    /// TABLE publication (sorted, de-duplicated).
    pub fn GetPublicationRelations<'mcx>(
        mcx: Mcx<'mcx>,
        pubid: Oid,
        pub_partopt: PublicationPartOpt,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `GetAllTablesPublications()` — oids of publications marked FOR ALL
    /// TABLES.
    pub fn GetAllTablesPublications<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `GetAllTablesPublicationRelations(pubviaroot)` — all relations published
    /// by FOR ALL TABLES publication(s).
    pub fn GetAllTablesPublicationRelations<'mcx>(
        mcx: Mcx<'mcx>,
        pubviaroot: bool,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `GetPublicationSchemas(pubid)` — schema oids of a FOR TABLES IN SCHEMA
    /// publication.
    pub fn GetPublicationSchemas<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `GetSchemaPublications(schemaid)` — publication oids associated with a
    /// schema.
    pub fn GetSchemaPublications<'mcx>(mcx: Mcx<'mcx>, schemaid: Oid) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `GetSchemaPublicationRelations(schemaid, pub_partopt)` — publishable
    /// relation oids in a schema.
    pub fn GetSchemaPublicationRelations<'mcx>(
        mcx: Mcx<'mcx>,
        schemaid: Oid,
        pub_partopt: PublicationPartOpt,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `GetAllSchemaPublicationRelations(pubid, pub_partopt)` — all relations
    /// published by a FOR TABLES IN SCHEMA publication.
    pub fn GetAllSchemaPublicationRelations<'mcx>(
        mcx: Mcx<'mcx>,
        pubid: Oid,
        pub_partopt: PublicationPartOpt,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `GetPubPartitionOptionRelations(result, pub_partopt, relid)` — expand
    /// `relid` according to the partition option, appending to `result`.
    pub fn GetPubPartitionOptionRelations<'mcx>(
        mcx: Mcx<'mcx>,
        result: PgVec<'mcx, Oid>,
        pub_partopt: PublicationPartOpt,
        relid: Oid,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `GetTopMostAncestorInPublication(puboid, ancestors, &ancestor_level)` —
    /// returns `(topmost_relid, ancestor_level)`. `topmost_relid` is
    /// `InvalidOid` when none.
    pub fn GetTopMostAncestorInPublication<'mcx>(
        mcx: Mcx<'mcx>,
        puboid: Oid,
        ancestors: &[Oid],
    ) -> PgResult<(Oid, i32)>
);

/* ==========================================================================
 * Predicates / column-list helpers.
 * ========================================================================== */

seam_core::seam!(
    /// `is_publishable_relation(rel)`.
    pub fn is_publishable_relation(rel: &Relation<'_>) -> PgResult<bool>
);

seam_core::seam!(
    /// `is_schema_publication(pubid)`.
    pub fn is_schema_publication<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `check_and_fetch_column_list(pub, relid, mcxt, &cols)`: returns
    /// `(found, cols)`; `cols` is the accumulated column bitmap (the C in/out
    /// `Bitmapset **`), `None` when no column list applies. `prior` is the
    /// existing accumulator the C passes in (`*cols`).
    pub fn check_and_fetch_column_list<'mcx>(
        mcx: Mcx<'mcx>,
        pubid: Oid,
        pub_alltables: bool,
        relid: Oid,
        prior: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    ) -> PgResult<(bool, Option<PgBox<'mcx, Bitmapset<'mcx>>>)>
);

seam_core::seam!(
    /// `pub_collist_validate(targetrel, columns)` — validate the column-name
    /// `String` nodes and return the 0-based attnum [`Bitmapset`].
    pub fn pub_collist_validate<'mcx>(
        mcx: Mcx<'mcx>,
        targetrel: &Relation<'mcx>,
        columns: &[mcx::PgBox<'mcx, Node<'mcx>>],
    ) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `pub_collist_to_bitmapset(columns, pubcols, mcxt)` — add the int2vector
    /// element attnums (the raw `int2vector` varlena bytes `pubcols`) to the
    /// running set `columns`.
    pub fn pub_collist_to_bitmapset<'mcx>(
        mcx: Mcx<'mcx>,
        columns: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
        pubcols: &[u8],
    ) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `pub_form_cols_map(relation, include_gencols_type)` — bitmap of the
    /// relation's published columns.
    pub fn pub_form_cols_map<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &Relation<'mcx>,
        include_gencols_type: PublishGencolsType,
    ) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>>
);

/* ==========================================================================
 * Mutators (the keystone surface publicationcmds.c needs).
 * ========================================================================== */

seam_core::seam!(
    /// `publication_add_relation(pubid, pri, if_not_exists)` — insert a
    /// `pg_publication_rel` mapping; returns the new object's [`ObjectAddress`]
    /// (or `InvalidObjectAddress` when it already exists and `if_not_exists`).
    /// `where_clause` is the optional row-filter; `columns` is the optional
    /// column-name `String`-node list. Mirrors `PublicationRelInfo`.
    pub fn publication_add_relation<'mcx>(
        mcx: Mcx<'mcx>,
        pubid: Oid,
        targetrel: &Relation<'mcx>,
        where_clause: Option<&Node<'mcx>>,
        columns: Option<&[mcx::PgBox<'mcx, Node<'mcx>>]>,
        if_not_exists: bool,
    ) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `publication_add_schema(pubid, schemaid, if_not_exists)` — insert a
    /// `pg_publication_namespace` mapping; returns the new object's
    /// [`ObjectAddress`] (or `InvalidObjectAddress`).
    pub fn publication_add_schema<'mcx>(
        mcx: Mcx<'mcx>,
        pubid: Oid,
        schemaid: Oid,
        if_not_exists: bool,
    ) -> PgResult<ObjectAddress>
);

/* ==========================================================================
 * SRF row builder (the portable core of `pg_get_publication_tables`).
 * ========================================================================== */

seam_core::seam!(
    /// `gather_publication_tables` + `build_publication_table_rows` — the
    /// portable body of `pg_get_publication_tables(pubnames text[])`: given the
    /// publication names, returns one [`PublicationTableRow`] per published
    /// table (pubid, relid, attrs int2vector bytes, qual pg_node_tree bytes).
    /// The SQL SRF wrapper that adapts this to the per-call `FuncCallContext`
    /// protocol is NOT installed here (that protocol is unported); a future SRF
    /// owner calls this to produce the rows.
    pub fn build_publication_table_rows<'mcx>(
        mcx: Mcx<'mcx>,
        pubnames: &[&str],
    ) -> PgResult<PgVec<'mcx, PublicationTableRow<'mcx>>>
);
