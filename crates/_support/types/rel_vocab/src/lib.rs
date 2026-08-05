#[derive(Debug)]
pub struct RangeVar<'a> {
    pub catalogname: Option<&'a str>,
    pub schemaname: Option<&'a str>,
    pub relname: &'a str,
    pub inh: bool,
    pub relpersistence: u8,
    pub location: i32,
}
