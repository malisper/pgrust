fn main() {
    println!(
        "cargo:rustc-env=PGRUST_TARGET_TRIPLE={}",
        std::env::var("TARGET").unwrap()
    );
}
