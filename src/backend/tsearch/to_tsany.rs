use crate::backend::parser::CatalogLookup;
use crate::include::catalog::{PgTsConfigMapRow, PgTsConfigRow, PgTsDictRow};
use crate::include::nodes::tsearch::{TsLexeme, TsQuery, TsVector};

// :HACK: Preserve the historical root text-search path while implementation
// lives in `pgrust_expr`. The old root API accepts analyzer `CatalogLookup`;
// adapt it to the narrower expression catalog service here.
struct ExprCatalogAdapter<'a>(&'a dyn CatalogLookup);

impl pgrust_expr::ExprCatalogLookup for ExprCatalogAdapter<'_> {
    fn ts_config_rows(&self) -> Vec<PgTsConfigRow> {
        self.0.ts_config_rows()
    }

    fn ts_dict_rows(&self) -> Vec<PgTsDictRow> {
        self.0.ts_dict_rows()
    }

    fn ts_config_map_rows(&self) -> Vec<PgTsConfigMapRow> {
        self.0.ts_config_map_rows()
    }
}

fn adapt_catalog<T>(
    catalog: Option<&dyn CatalogLookup>,
    f: impl FnOnce(Option<&dyn pgrust_expr::ExprCatalogLookup>) -> T,
) -> T {
    let adapter = catalog.map(ExprCatalogAdapter);
    let catalog = adapter
        .as_ref()
        .map(|adapter| adapter as &dyn pgrust_expr::ExprCatalogLookup);
    f(catalog)
}

pub(crate) fn to_tsvector_with_config_name(
    config_name: Option<&str>,
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TsVector, String> {
    adapt_catalog(catalog, |catalog| {
        pgrust_expr::backend::tsearch::to_tsvector_with_config_name(config_name, text, catalog)
    })
}

pub(crate) fn tsvector_lexemes_with_config_name(
    config_name: Option<&str>,
    text: &str,
    start_position: u16,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<(Vec<TsLexeme>, u16), String> {
    adapt_catalog(catalog, |catalog| {
        pgrust_expr::backend::tsearch::tsvector_lexemes_with_config_name(
            config_name,
            text,
            start_position,
            catalog,
        )
    })
}

pub(crate) fn to_tsquery_with_config_name(
    config_name: Option<&str>,
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TsQuery, String> {
    adapt_catalog(catalog, |catalog| {
        pgrust_expr::backend::tsearch::to_tsquery_with_config_name(config_name, text, catalog)
    })
}

pub(crate) fn plainto_tsquery_with_config_name(
    config_name: Option<&str>,
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TsQuery, String> {
    adapt_catalog(catalog, |catalog| {
        pgrust_expr::backend::tsearch::plainto_tsquery_with_config_name(config_name, text, catalog)
    })
}

pub(crate) fn phraseto_tsquery_with_config_name(
    config_name: Option<&str>,
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TsQuery, String> {
    adapt_catalog(catalog, |catalog| {
        pgrust_expr::backend::tsearch::phraseto_tsquery_with_config_name(config_name, text, catalog)
    })
}

pub(crate) fn websearch_to_tsquery_with_config_name(
    config_name: Option<&str>,
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TsQuery, String> {
    adapt_catalog(catalog, |catalog| {
        pgrust_expr::backend::tsearch::websearch_to_tsquery_with_config_name(
            config_name,
            text,
            catalog,
        )
    })
}

pub(crate) fn ts_lexize_with_dictionary_name(
    dictionary_name: &str,
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Option<Vec<String>>, String> {
    adapt_catalog(catalog, |catalog| {
        pgrust_expr::backend::tsearch::ts_lexize_with_dictionary_name(
            dictionary_name,
            text,
            catalog,
        )
    })
}
