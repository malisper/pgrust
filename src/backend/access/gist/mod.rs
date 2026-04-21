mod build;
mod insert;
mod page;
mod scan;
mod state;
mod support;
mod tuple;
mod vacuum;
pub mod wal;

use crate::include::access::amapi::IndexAmRoutine;

pub fn gist_am_handler() -> IndexAmRoutine {
    IndexAmRoutine {
        amstrategies: 0,
        amsupport: 12,
        amcanorder: false,
        amcanorderbyop: true,
        amcanhash: false,
        amconsistentordering: false,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: true,
        amoptionalkey: true,
        amsearcharray: false,
        amsearchnulls: true,
        amstorage: true,
        amclusterable: true,
        ampredlocks: false,
        ambuild: Some(build::gistbuild),
        ambuildempty: Some(build::gistbuildempty),
        aminsert: Some(insert::gistinsert),
        ambeginscan: Some(scan::gistbeginscan),
        amrescan: Some(scan::gistrescan),
        amgettuple: Some(scan::gistgettuple),
        amendscan: Some(scan::gistendscan),
        ambulkdelete: Some(vacuum::gistbulkdelete),
        amvacuumcleanup: Some(vacuum::gistvacuumcleanup),
    }
}

#[cfg(test)]
mod tests {
    use super::gist_am_handler;

    #[test]
    fn gist_handler_advertises_order_by_operator_support() {
        let am = gist_am_handler();

        assert!(!am.amcanorder);
        assert!(am.amcanorderbyop);
    }
}
