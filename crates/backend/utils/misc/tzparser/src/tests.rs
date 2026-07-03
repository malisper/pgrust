use super::*;
use adt_datetime::consts::{DTZ, DYNTZ, TZ};
use adt_datetime::decode::datebsearch;
use adt_datetime::tz::{zoneabbrevtbl, FetchDynamicTimeZone, InstallTimeZoneAbbrevs};
use adt_datetime::DateTimeErrorExtra;

fn c_install_dir() -> Option<&'static str> {
    [
        "/tmp/pgrust_pginstall/share/postgresql/timezonesets",
        "/opt/homebrew/share/postgresql@18/timezonesets",
    ]
    .into_iter()
    .find(|d| std::path::Path::new(d).is_dir())
}

fn scratch_dir(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("tzparser-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_str().unwrap().to_string()
}

fn load_err(dir: &str, file: &str) -> guc::GucCheckError {
    guc::reset_guc_check_error();
    assert!(load_tzoffsets_from(dir, file).is_none());
    guc::take_guc_check_error()
}

fn find(tbl: &'static ZoneAbbrevTable, abbrev: &str) -> &'static adt_datetime::DateTkn {
    datebsearch(abbrev.as_bytes(), tbl.abbrevs).unwrap_or_else(|| panic!("{abbrev} not found"))
}

#[test]
fn default_matches_c_install() {
    let Some(dir) = c_install_dir() else { return };
    let tbl = load_tzoffsets_from(dir, "Default").expect("Default parses");
    assert_eq!(tbl.abbrevs.len(), 195);
    let est = find(tbl, "est");
    assert_eq!((est.typ as i32, est.value), (TZ, -18000));
    let edt = find(tbl, "edt");
    assert_eq!((edt.typ as i32, edt.value), (DTZ, -14400));
    let utc = find(tbl, "utc");
    assert_eq!((utc.typ as i32, utc.value), (TZ, 0));
    assert!(datebsearch(b"EST", tbl.abbrevs).is_none());
    assert!(adt_datetime::decode::CheckDateTokenTable(tbl.abbrevs));
}

#[test]
fn australia_and_india_override_default() {
    let Some(dir) = c_install_dir() else { return };
    let au = load_tzoffsets_from(dir, "Australia").expect("Australia parses");
    let est = find(au, "est");
    assert_eq!((est.typ as i32, est.value), (TZ, 36000));
    let cst = find(au, "cst");
    assert_eq!(cst.value, 34200);

    let india = load_tzoffsets_from(dir, "India").expect("India parses");
    let ist = find(india, "ist");
    assert_eq!((ist.typ as i32, ist.value), (TZ, 19800));
    assert_eq!(find(india, "est").value, -18000);
}

fn setup_tz_engine() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo");
        pgtz::init_seams();
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
    });
}

#[test]
fn dynamic_abbrev_resolves_through_pgtz() {
    setup_tz_engine();
    let dir = scratch_dir("dyn");
    std::fs::write(
        format!("{dir}/Dyn"),
        "FOO  America/New_York  # dynamic\nBAR  Not/AZone\nEST -18000\n",
    )
    .unwrap();
    let tbl = load_tzoffsets_from(&dir, "Dyn").unwrap();
    let foo = find(tbl, "foo");
    assert_eq!(foo.typ as i32, DYNTZ);
    let mut extra = DateTimeErrorExtra::default();
    let tz = FetchDynamicTimeZone(tbl, foo, &mut extra).expect("America/New_York loads");
    assert!(FetchDynamicTimeZone(tbl, foo, &mut extra).is_some());
    assert_eq!(
        adt_datetime::tz::pg_get_timezone_name(tz),
        Some("America/New_York")
    );

    let bar = find(tbl, "bar");
    let mut extra = DateTimeErrorExtra::default();
    assert!(FetchDynamicTimeZone(tbl, bar, &mut extra).is_none());
    assert_eq!(extra.dtee_timezone, Some(&b"Not/AZone"[..]));
    assert_eq!(extra.dtee_abbrev, Some(&b"bar"[..]));
}

#[test]
fn install_makes_table_live() {
    let dir = scratch_dir("install");
    std::fs::write(format!("{dir}/Mini"), "ZZZTEST 3600 D\n").unwrap();
    let tbl = load_tzoffsets_from(&dir, "Mini").unwrap();
    InstallTimeZoneAbbrevs(tbl);
    let live = zoneabbrevtbl().expect("table installed");
    assert_eq!(find(live, "zzztest").value, 3600);
}

#[test]
fn include_recursion_and_override() {
    let dir = scratch_dir("inc");
    std::fs::write(format!("{dir}/Base"), "AAA 100\nBBB 200 D\n").unwrap();
    std::fs::write(
        format!("{dir}/Top"),
        "@INCLUDE Base\n@OVERRIDE\nAAA 300\nCCC Asia/Tokyo\n",
    )
    .unwrap();
    let tbl = load_tzoffsets_from(&dir, "Top").unwrap();
    assert_eq!(tbl.abbrevs.len(), 3);
    assert_eq!(find(tbl, "aaa").value, 300);
    assert_eq!((find(tbl, "bbb").typ as i32, find(tbl, "bbb").value), (DTZ, 200));
    assert_eq!(find(tbl, "ccc").typ as i32, DYNTZ);
}

#[test]
fn include_cycle_hits_recursion_limit() {
    let dir = scratch_dir("cycle");
    std::fs::write(format!("{dir}/Ping"), "@INCLUDE Pong\n").unwrap();
    std::fs::write(format!("{dir}/Pong"), "@INCLUDE Ping\n").unwrap();
    let e = load_err(&dir, "Ping");
    assert_eq!(
        e.message.as_deref(),
        Some("time zone file recursion limit exceeded in file \"Ping\"")
    );
}

#[test]
fn duplicate_without_override_conflicts() {
    let dir = scratch_dir("dup");
    std::fs::write(format!("{dir}/Base"), "AAA 100\n").unwrap();
    std::fs::write(format!("{dir}/Top"), "@INCLUDE Base\nAAA 300\n").unwrap();
    let e = load_err(&dir, "Top");
    assert_eq!(
        e.message.as_deref(),
        Some("time zone abbreviation \"aaa\" is multiply defined")
    );
    assert_eq!(
        e.detail.as_deref(),
        Some("Entry in time zone file \"Base\", line 1, conflicts with entry in file \"Top\", line 2.")
    );

    std::fs::write(format!("{dir}/Same"), "@INCLUDE Base\nAAA 100\n").unwrap();
    assert!(load_tzoffsets_from(&dir, "Same").is_some());
}

#[test]
fn syntax_errors_carry_line_numbers() {
    let dir = scratch_dir("syn");
    std::fs::write(format!("{dir}/Bad"), "# c\nAAA 100\nAAA 100 D trailing\n").unwrap();
    assert_eq!(
        load_err(&dir, "Bad").message.as_deref(),
        Some("invalid syntax in time zone file \"Bad\", line 3")
    );

    std::fs::write(format!("{dir}/Num"), "AAA 12x3\n").unwrap();
    assert_eq!(
        load_err(&dir, "Num").message.as_deref(),
        Some("invalid number for time zone offset in time zone file \"Num\", line 1")
    );

    std::fs::write(format!("{dir}/NoOff"), "AAA\n").unwrap();
    assert_eq!(
        load_err(&dir, "NoOff").message.as_deref(),
        Some("missing time zone offset in time zone file \"NoOff\", line 1")
    );

    std::fs::write(format!("{dir}/NoInc"), "@INCLUDE   \n").unwrap();
    assert_eq!(
        load_err(&dir, "NoInc").message.as_deref(),
        Some("@INCLUDE without file name in time zone file \"NoInc\", line 1")
    );
}

#[test]
fn validation_errors() {
    let dir = scratch_dir("val");
    std::fs::write(format!("{dir}/Long"), "ABCDEFGHIJK 100\n").unwrap();
    assert_eq!(
        load_err(&dir, "Long").message.as_deref(),
        Some("time zone abbreviation \"ABCDEFGHIJK\" is too long (maximum 10 characters) in time zone file \"Long\", line 1")
    );

    std::fs::write(format!("{dir}/Range"), "AAA 50401\n").unwrap();
    assert_eq!(
        load_err(&dir, "Range").message.as_deref(),
        Some("time zone offset 50401 is out of range in time zone file \"Range\", line 1")
    );
    std::fs::write(format!("{dir}/Edge"), "AAA -50400\nBBB +50400\n").unwrap();
    assert!(load_tzoffsets_from(&dir, "Edge").is_some());
}

#[test]
fn filename_and_open_failures() {
    let dir = scratch_dir("open");
    // Non-alpha name at depth 0: guc.c's generic message stands (no errmsg).
    assert_eq!(load_err(&dir, "no.such").message, None);
    std::fs::write(format!("{dir}/Esc"), "@INCLUDE ../etc/passwd\n").unwrap();
    assert_eq!(
        load_err(&dir, "Esc").message.as_deref(),
        Some("invalid time zone file name \"../etc/passwd\"")
    );

    // Missing file at depth 0 with a readable directory: silent (ENOENT).
    assert_eq!(load_err(&dir, "Nope").message, None);
    std::fs::write(format!("{dir}/IncNope"), "@INCLUDE Nope\n").unwrap();
    let e = load_err(&dir, "IncNope");
    assert!(e.message.unwrap().starts_with("could not read time zone file \"Nope\":"));

    let e = load_err(&format!("{dir}/absent"), "Default");
    assert!(e.message.unwrap().starts_with("could not open directory"));
    assert!(e.hint.is_some());
}

#[test]
fn line_too_long() {
    let dir = scratch_dir("longline");
    let mut contents = String::from("AAA 100\n# ");
    contents.push_str(&"x".repeat(1500));
    contents.push('\n');
    std::fs::write(format!("{dir}/Big"), contents).unwrap();
    assert_eq!(
        load_err(&dir, "Big").message.as_deref(),
        Some("line is too long in time zone file \"Big\", line 2")
    );
}

#[test]
fn strtol_semantics() {
    assert_eq!(strtol10(b"123"), Some(123));
    assert_eq!(strtol10(b"+123"), Some(123));
    assert_eq!(strtol10(b"-14400"), Some(-14400));
    assert_eq!(strtol10(b"12x"), None);
    assert_eq!(strtol10(b"-"), None);
    assert_eq!(strtol10(b""), None);
    // C: strtol saturates at LONG_MAX/LONG_MIN, then truncates into the int
    // field.
    assert_eq!(strtol10(b"99999999999999999999999999"), Some(i64::MAX as i32));
    assert_eq!(strtol10(b"4294967296"), Some(0));
}

#[test]
fn directive_prefix_matching_is_c_loose() {
    // C matches @INCLUDE/@OVERRIDE by prefix, no word boundary.
    let dir = scratch_dir("loose");
    std::fs::write(format!("{dir}/Base"), "AAA 100\n").unwrap();
    std::fs::write(format!("{dir}/Loose"), "@includeBase\n@overrideX\nAAA 200\n").unwrap();
    let tbl = load_tzoffsets_from(&dir, "Loose").unwrap();
    assert_eq!(find(tbl, "aaa").value, 200);
}
