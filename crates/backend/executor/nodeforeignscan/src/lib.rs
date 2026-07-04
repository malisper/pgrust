#![allow(non_snake_case)]

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: nodeForeignscan.c {what} (no FDW implementation exists)")
}

pub fn ExecInitForeignScan() -> ! {
    unported("ExecInitForeignScan")
}

pub fn ExecForeignScan() -> ! {
    unported("ExecForeignScan")
}

pub fn ExecEndForeignScan() -> ! {
    unported("ExecEndForeignScan")
}

pub fn ExecReScanForeignScan() -> ! {
    unported("ExecReScanForeignScan")
}
