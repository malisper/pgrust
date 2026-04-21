use crate::include::catalog::{PgPublicationNamespaceRow, PgPublicationRelRow, PgPublicationRow};

pub fn sort_pg_publication_rows(rows: &mut [PgPublicationRow]) {
    rows.sort_by(|left, right| {
        left.pubname
            .cmp(&right.pubname)
            .then_with(|| left.oid.cmp(&right.oid))
    });
}

pub fn sort_pg_publication_rel_rows(rows: &mut [PgPublicationRelRow]) {
    rows.sort_by(|left, right| {
        left.prpubid
            .cmp(&right.prpubid)
            .then_with(|| left.prrelid.cmp(&right.prrelid))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}

pub fn sort_pg_publication_namespace_rows(rows: &mut [PgPublicationNamespaceRow]) {
    rows.sort_by(|left, right| {
        left.pnpubid
            .cmp(&right.pnpubid)
            .then_with(|| left.pnnspid.cmp(&right.pnnspid))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
