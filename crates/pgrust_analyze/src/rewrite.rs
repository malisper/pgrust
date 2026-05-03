use pgrust_nodes::parsenodes::ParseError;
use pgrust_nodes::primnodes::{Expr, RelationDesc};

use crate::BoundRelation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RlsWriteCheckSource {
    Insert,
    Update,
    SelectVisibility,
    ConflictUpdateVisibility,
    MergeUpdateVisibility,
    MergeDeleteVisibility,
    ViewCheckOption(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RlsWriteCheck {
    pub expr: Expr,
    pub display_exprs: Vec<Expr>,
    pub policy_name: Option<String>,
    pub source: RlsWriteCheckSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetRlsState {
    pub visibility_quals: Vec<Expr>,
    pub write_checks: Vec<RlsWriteCheck>,
    pub depends_on_row_security: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonUpdatableViewColumnReason {
    SystemColumn,
    NotBaseRelationColumn,
}

impl NonUpdatableViewColumnReason {
    pub fn detail(self) -> &'static str {
        match self {
            NonUpdatableViewColumnReason::SystemColumn => {
                "View columns that refer to system columns are not updatable."
            }
            NonUpdatableViewColumnReason::NotBaseRelationColumn => {
                "View columns that are not columns of their base relation are not updatable."
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonUpdatableViewColumn {
    pub relation_name: String,
    pub reason: NonUpdatableViewColumnReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewDmlEvent {
    Insert,
    Update,
    Delete,
}

impl ViewDmlEvent {
    pub fn rule_event_code(self) -> char {
        match self {
            ViewDmlEvent::Update => '2',
            ViewDmlEvent::Insert => '3',
            ViewDmlEvent::Delete => '4',
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ViewRuleEventClassification {
    pub unconditional_instead: bool,
    pub conditional_instead: bool,
    pub also: bool,
}

#[derive(Debug, Clone)]
pub struct ResolvedAutoViewTarget {
    pub base_relation: BoundRelation,
    pub base_inh: bool,
    pub visible_output_exprs: Vec<Expr>,
    pub combined_predicate: Option<Expr>,
    pub has_security_barrier: bool,
    pub updatable_column_map: Vec<Option<usize>>,
    pub non_updatable_column_reasons: Vec<Option<NonUpdatableViewColumn>>,
    pub local_updatable_column_map: Vec<Option<usize>>,
    pub local_non_updatable_column_reasons: Vec<Option<NonUpdatableViewColumn>>,
    pub privilege_contexts: Vec<ViewPrivilegeContext>,
    pub all_view_predicates: Vec<ViewCheck>,
    pub view_check_options: Vec<ViewCheck>,
}

#[derive(Debug, Clone)]
pub struct ViewPrivilegeContext {
    pub relation: BoundRelation,
    pub check_as_user_oid: Option<u32>,
    pub column_map: Vec<Option<usize>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewCheck {
    pub view_name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewDmlRewriteError {
    Parse(ParseError),
    UnsupportedViewShape(String),
    UnsupportedViewShapeForView {
        relation_name: String,
        detail: String,
    },
    NestedUserRuleMix(String),
    RecursiveView(String),
    DeferredFeature(String),
    NonUpdatableColumn {
        relation_name: String,
        column_name: String,
        reason: NonUpdatableViewColumnReason,
    },
    MultipleAssignments(String),
}

impl From<ParseError> for ViewDmlRewriteError {
    fn from(err: ParseError) -> Self {
        ViewDmlRewriteError::Parse(err)
    }
}

impl ViewDmlRewriteError {
    pub fn detail(&self) -> String {
        match self {
            ViewDmlRewriteError::Parse(err) => err.to_string(),
            ViewDmlRewriteError::UnsupportedViewShapeForView { detail, .. } => detail.clone(),
            ViewDmlRewriteError::UnsupportedViewShape(detail)
            | ViewDmlRewriteError::NestedUserRuleMix(detail)
            | ViewDmlRewriteError::DeferredFeature(detail) => detail.clone(),
            ViewDmlRewriteError::RecursiveView(_) => {
                "Views that directly or indirectly reference themselves are not automatically updatable."
                    .into()
            }
            ViewDmlRewriteError::NonUpdatableColumn { reason, .. } => reason.detail().into(),
            ViewDmlRewriteError::MultipleAssignments(_) => String::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ViewUpdatability {
    pub insertable: bool,
    pub updatable: bool,
    pub deletable: bool,
    pub trigger_insertable: bool,
    pub trigger_updatable: bool,
    pub trigger_deletable: bool,
    pub relation_desc: Option<RelationDesc>,
}
