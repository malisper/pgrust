#[test]
fn dbg_gucfile() {
    let inputs: Vec<Vec<u8>> = vec![
        b"\x00".to_vec(),
        b"\x00\n".to_vec(),
        b"\x00# comment only\n".to_vec(),
        b"\x00work_mem = '64MB'\n".to_vec(),
    ];
    for inp in inputs {
        eprintln!(">>> {:?}", String::from_utf8_lossy(&inp));
        decoder_fuzz::guc_file_diff(&inp);
        eprintln!("    ok");
    }
}
