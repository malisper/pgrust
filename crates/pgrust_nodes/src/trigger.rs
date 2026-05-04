use crate::datum::Value;
use crate::parsenodes::{TriggerLevel, TriggerTiming};
use crate::primnodes::RelationDesc;

#[derive(Debug, Clone, Default)]
pub struct TriggerTransitionCapture {
    pub old_rows: Vec<Vec<Value>>,
    pub new_rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerOperation {
    Insert,
    Update,
    Delete,
    Truncate,
}

#[derive(Debug, Clone)]
pub struct TriggerTransitionTable {
    pub name: String,
    pub desc: RelationDesc,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone)]
pub struct TriggerCallContext {
    pub relation_desc: RelationDesc,
    pub relation_oid: u32,
    pub table_name: String,
    pub table_schema: String,
    pub trigger_name: String,
    pub trigger_args: Vec<String>,
    pub timing: TriggerTiming,
    pub level: TriggerLevel,
    pub op: TriggerOperation,
    pub new_row: Option<Vec<Value>>,
    pub old_row: Option<Vec<Value>>,
    pub transition_tables: Vec<TriggerTransitionTable>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTriggerCallContext {
    pub event: String,
    pub tag: String,
    pub ddl_commands: Vec<EventTriggerDdlCommandRow>,
    pub dropped_objects: Vec<EventTriggerDroppedObjectRow>,
    pub table_rewrite_relation_oid: Option<u32>,
    pub table_rewrite_relation_name: Option<String>,
    pub table_rewrite_reason: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTriggerDdlCommandRow {
    pub command_tag: String,
    pub object_type: String,
    pub schema_name: Option<String>,
    pub object_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTriggerDroppedObjectRow {
    pub classid: u32,
    pub objid: u32,
    pub objsubid: i32,
    pub original: bool,
    pub normal: bool,
    pub is_temporary: bool,
    pub object_type: String,
    pub schema_name: Option<String>,
    pub object_name: Option<String>,
    pub object_identity: String,
    pub address_names: Vec<String>,
    pub address_args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerFunctionResult {
    SkipRow,
    ReturnNew(Vec<Value>),
    ReturnOld(Vec<Value>),
    NoValue,
}
