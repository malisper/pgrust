mod build;
mod insert;
mod page;
mod quad_box;
mod scan;
mod state;
mod support;
mod tuple;
mod vacuum;

use crate::include::access::amapi::IndexAmRoutine;

pub fn spgist_am_handler() -> IndexAmRoutine {
    IndexAmRoutine {
        amstrategies: 0,
        amsupport: 7,
        amcanorder: false,
        amcanorderbyop: true,
        amcanhash: false,
        amconsistentordering: false,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: false,
        amoptionalkey: true,
        amsearcharray: false,
        amsearchnulls: false,
        amstorage: true,
        amclusterable: false,
        ampredlocks: false,
        ambuild: Some(build::spgbuild),
        ambuildempty: Some(build::spgbuildempty),
        aminsert: Some(insert::spginsert),
        ambeginscan: Some(scan::spgbeginscan),
        amrescan: Some(scan::spgrescan),
        amgettuple: Some(scan::spggettuple),
        amendscan: Some(scan::spgendscan),
        ambulkdelete: Some(vacuum::spgbulkdelete),
        amvacuumcleanup: Some(vacuum::spgvacuumcleanup),
    }
}

#[cfg(test)]
mod tests {
    use super::spgist_am_handler;

    #[test]
    fn spgist_handler_advertises_native_properties() {
        let am = spgist_am_handler();

        assert_eq!(am.amsupport, 7);
        assert!(am.amcanorderbyop);
        assert!(!am.amcanmulticol);
        assert!(!am.amcanbackward);
        assert!(!am.amclusterable);
        assert!(!am.amcanunique);
        assert!(am.amoptionalkey);
        assert!(!am.amsearchnulls);
        assert!(am.amstorage);
    }
}
