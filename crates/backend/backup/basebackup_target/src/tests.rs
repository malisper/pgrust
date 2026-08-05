use super::*;
use ::mcx::MemoryContext;
use ::sink::{Bbsink, BbsinkOps, BbsinkState};
use ::types_core::{Size, TimeLineID, XLogRecPtr};

/// Minimal leaf ops so a `Bbsink` can be built for `get_sink` tests.
struct NoopLeaf;
impl<'mcx> BbsinkOps<'mcx> for NoopLeaf {
    fn begin_backup(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> { Ok(()) }
    fn begin_archive(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState, _: &str) -> PgResult<()> { Ok(()) }
    fn archive_contents(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState, _: Size) -> PgResult<()> { Ok(()) }
    fn end_archive(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> { Ok(()) }
    fn begin_manifest(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> { Ok(()) }
    fn manifest_contents(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState, _: Size) -> PgResult<()> { Ok(()) }
    fn end_manifest(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> { Ok(()) }
    fn end_backup(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState, _: XLogRecPtr, _: TimeLineID) -> PgResult<()> { Ok(()) }
    fn cleanup(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> { Ok(()) }
}

#[test]
fn blackhole_rejects_detail_and_passes_through() {
    let ctx = MemoryContext::new("target test");
    let mcx = ctx.mcx();

    // blackhole rejects a supplied detail.
    assert!(BaseBackupGetTargetHandle("blackhole", Some("x")).is_err());

    // blackhole with no detail resolves; get_sink returns the successor.
    let handle = BaseBackupGetTargetHandle("blackhole", None).unwrap();
    assert_eq!(handle.type_name, "blackhole");
    let leaf = Box::new(Bbsink::new(mcx, Box::new(NoopLeaf), None));
    let sink = BaseBackupGetSink(mcx, handle, leaf).unwrap();
    assert!(sink.next().is_none());
}

#[test]
fn server_requires_detail() {
    assert!(BaseBackupGetTargetHandle("server", None).is_err());
    let handle = BaseBackupGetTargetHandle("server", Some("/backups")).unwrap();
    assert_eq!(handle.type_name, "server");
    assert!(matches!(handle.detail_arg, TargetDetail::Server(ref d) if d == "/backups"));
}

#[test]
fn unrecognized_target_errors() {
    assert!(BaseBackupGetTargetHandle("nonesuch", None).is_err());
}

#[test]
fn add_target_updates_in_place() {
    struct Dummy;
    impl BaseBackupTarget for Dummy {
        fn check_detail(&self, _: &str, _: Option<&str>) -> PgResult<TargetDetail> {
            Ok(TargetDetail::None)
        }
        fn get_sink<'mcx>(
            &self,
            _: Mcx<'mcx>,
            next_sink: Box<Bbsink<'mcx>>,
            _: TargetDetail,
        ) -> PgResult<Box<Bbsink<'mcx>>> {
            Ok(next_sink)
        }
    }
    BaseBackupAddTarget("blackhole", Box::new(Dummy));
    // Still resolvable (name updated in place, not duplicated).
    let handle = BaseBackupGetTargetHandle("blackhole", Some("now-accepted")).unwrap();
    assert_eq!(handle.type_name, "blackhole");
}

#[test]
fn server_target_detail_contract() {
    // server requires a target detail (basebackup_target.c server_check_detail);
    // get_sink itself needs a live transaction + role infrastructure (the
    // pg_write_server_files check), so only handle resolution is unit-testable.
    assert!(BaseBackupGetTargetHandle("server", None).is_err());
    let handle = BaseBackupGetTargetHandle("server", Some("/backups")).unwrap();
    assert_eq!(handle.type_name, "server");
    assert!(matches!(handle.detail_arg, TargetDetail::Server(ref p) if p == "/backups"));
}
