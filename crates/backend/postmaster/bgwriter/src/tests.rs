#[test]
fn main_fn_matches_child_main_shape() {
    let f: fn(&types_startup::StartupData) -> ! = super::BackgroundWriterMain;
    let _ = f;
}
