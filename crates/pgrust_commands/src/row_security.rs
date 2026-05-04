use pgrust_nodes::parsenodes::AlterTableRowSecurityAction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowSecurityUpdate {
    pub relrowsecurity: Option<bool>,
    pub relforcerowsecurity: Option<bool>,
}

pub fn row_security_update_for_action(action: AlterTableRowSecurityAction) -> RowSecurityUpdate {
    match action {
        AlterTableRowSecurityAction::Enable => RowSecurityUpdate {
            relrowsecurity: Some(true),
            relforcerowsecurity: None,
        },
        AlterTableRowSecurityAction::Disable => RowSecurityUpdate {
            relrowsecurity: Some(false),
            relforcerowsecurity: None,
        },
        AlterTableRowSecurityAction::Force => RowSecurityUpdate {
            relrowsecurity: None,
            relforcerowsecurity: Some(true),
        },
        AlterTableRowSecurityAction::NoForce => RowSecurityUpdate {
            relrowsecurity: None,
            relforcerowsecurity: Some(false),
        },
    }
}
