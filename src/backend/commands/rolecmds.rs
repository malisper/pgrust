use std::collections::HashMap;
use std::sync::OnceLock;

use crate::backend::catalog::role_memberships::NewRoleMembership;
use crate::backend::catalog::roles::{RoleAttributes, find_role_by_name};
use crate::backend::parser::{
    AlterRoleAction, AlterRoleStatement, CreateRoleStatement, DropRoleStatement, ParseError,
    RoleOption,
};
use crate::include::catalog::{PG_DATABASE_OWNER_OID, PgAuthIdRow};
use crate::pgrust::auth::{AuthCatalog, AuthState};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use rand::RngCore;
use sha2::{Digest, Sha256};

static ROLE_SETTINGS: OnceLock<RwLock<HashMap<(u32, u32), HashMap<String, String>>>> =
    OnceLock::new();

fn role_settings_store() -> &'static RwLock<HashMap<(u32, u32), HashMap<String, String>>> {
    ROLE_SETTINGS.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn store_role_setting(database_oid: u32, role_oid: u32, name: String, value: Option<String>) {
    let mut settings = role_settings_store().write();
    let role_settings = settings.entry((database_oid, role_oid)).or_default();
    if let Some(value) = value {
        role_settings.insert(name, value);
    } else {
        role_settings.remove(&name);
    }
}

pub fn role_settings(database_oid: u32, role_oid: u32) -> HashMap<String, String> {
    role_settings_store()
        .read()
        .get(&(database_oid, role_oid))
        .cloned()
        .unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltRoleSpec {
    pub attrs: RoleAttributes,
    pub saw_sysid: bool,
    pub add_role_to: Vec<String>,
    pub role_members: Vec<String>,
    pub admin_members: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PasswordEncryption {
    Md5,
    ScramSha256,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PasswordSettings {
    pub encryption: PasswordEncryption,
    pub scram_iterations: u32,
}

impl Default for PasswordSettings {
    fn default() -> Self {
        Self {
            encryption: PasswordEncryption::ScramSha256,
            scram_iterations: 4096,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateRoleSelfGrant {
    pub inherit: bool,
    pub set: bool,
}

pub fn build_create_role_spec(
    stmt: &CreateRoleStatement,
    password_settings: PasswordSettings,
) -> Result<BuiltRoleSpec, ParseError> {
    let mut attrs = RoleAttributes {
        rolcanlogin: stmt.is_user,
        ..RoleAttributes::default()
    };
    let mut add_role_to = Vec::new();
    let mut role_members = Vec::new();
    let mut admin_members = Vec::new();
    let saw_sysid = apply_role_options(
        &mut attrs,
        &stmt.options,
        &mut add_role_to,
        &mut role_members,
        &mut admin_members,
        &stmt.role_name,
        password_settings,
    )?;
    Ok(BuiltRoleSpec {
        attrs,
        saw_sysid,
        add_role_to,
        role_members,
        admin_members,
    })
}

pub fn build_alter_role_spec(
    stmt: &AlterRoleStatement,
    existing: &PgAuthIdRow,
    password_settings: PasswordSettings,
) -> Result<Option<BuiltRoleSpec>, ParseError> {
    match &stmt.action {
        AlterRoleAction::Rename { .. } | AlterRoleAction::SetConfig { .. } => Ok(None),
        AlterRoleAction::Options(options) => {
            let mut attrs = RoleAttributes {
                rolsuper: existing.rolsuper,
                rolinherit: existing.rolinherit,
                rolcreaterole: existing.rolcreaterole,
                rolcreatedb: existing.rolcreatedb,
                rolcanlogin: existing.rolcanlogin,
                rolreplication: existing.rolreplication,
                rolbypassrls: existing.rolbypassrls,
                rolconnlimit: existing.rolconnlimit,
                rolpassword: existing.rolpassword.clone(),
            };
            let saw_sysid = apply_role_options(
                &mut attrs,
                options,
                &mut Vec::new(),
                &mut Vec::new(),
                &mut Vec::new(),
                &stmt.role_name,
                password_settings,
            )?;
            Ok(Some(BuiltRoleSpec {
                attrs,
                saw_sysid,
                add_role_to: Vec::new(),
                role_members: Vec::new(),
                admin_members: Vec::new(),
            }))
        }
    }
}

pub fn can_rename_role(auth: &AuthState, target_oid: u32, catalog: &AuthCatalog) -> bool {
    let Some(current) = catalog.role_by_oid(auth.current_user_oid()) else {
        return false;
    };
    let target = catalog.role_by_oid(target_oid);
    current.rolsuper
        || (current.rolcreaterole
            && target.is_none_or(|row| !row.rolsuper)
            && auth.has_admin_option(target_oid, catalog))
}

pub fn normalize_drop_role_names(stmt: &DropRoleStatement) -> Vec<String> {
    let mut names = Vec::new();
    for role_name in &stmt.role_names {
        if !names
            .iter()
            .any(|existing: &String| existing.eq_ignore_ascii_case(role_name))
        {
            names.push(role_name.clone());
        }
    }
    names
}

pub fn role_management_error(message: impl Into<String>) -> ParseError {
    ParseError::UnexpectedToken {
        expected: "role management operation",
        actual: message.into(),
    }
}

impl PasswordEncryption {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "md5" => Some(Self::Md5),
            "scram-sha-256" => Some(Self::ScramSha256),
            _ => None,
        }
    }
}

pub fn parse_scram_iterations(value: &str) -> Option<u32> {
    let trimmed = value.trim();
    let parsed = trimmed.parse::<u32>().ok()?;
    (parsed > 0).then_some(parsed)
}

fn normalize_role_password(
    password: Option<&str>,
    _role_name: &str,
    settings: PasswordSettings,
) -> Result<Option<String>, ParseError> {
    let Some(password) = password else {
        return Ok(None);
    };
    if password.is_empty() {
        push_empty_password_notice();
        return Ok(None);
    }
    if is_md5_encrypted_password(password) {
        push_md5_password_warning();
        return Ok(Some(password.to_string()));
    }
    if let Some(secret) = parse_scram_secret(password) {
        if password.len() > 512 {
            return Err(ParseError::DetailedError {
                message: "encrypted password is too long".into(),
                detail: Some("Encrypted passwords must be no longer than 512 bytes.".into()),
                hint: None,
                sqlstate: "22023",
            });
        }
        if scram_secret_matches_password(&secret, "") {
            push_empty_password_notice();
            return Ok(None);
        }
        return Ok(Some(password.to_string()));
    }

    match settings.encryption {
        PasswordEncryption::Md5 => Err(ParseError::DetailedError {
            message: "password encryption failed: unsupported".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        }),
        PasswordEncryption::ScramSha256 => Ok(Some(build_scram_secret(
            password,
            settings.scram_iterations,
        ))),
    }
}

fn push_empty_password_notice() {
    crate::backend::utils::misc::notices::push_notice(
        "empty string is not a valid password, clearing password",
    );
}

fn push_md5_password_warning() {
    crate::backend::utils::misc::notices::push_backend_notice_with_hint(
        "WARNING",
        "01000",
        "setting an MD5-encrypted password",
        Some("MD5 password support is deprecated and will be removed in a future release of PostgreSQL.".into()),
        Some("Refer to the PostgreSQL documentation for details about migrating to another password type.".into()),
        None,
    );
}

fn is_md5_encrypted_password(value: &str) -> bool {
    value.len() == 35
        && value
            .strip_prefix("md5")
            .is_some_and(|digest| digest.chars().all(|ch| ch.is_ascii_hexdigit()))
}

#[derive(Debug, Clone)]
struct ScramSecret {
    iterations: u32,
    salt: Vec<u8>,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
}

fn parse_scram_secret(value: &str) -> Option<ScramSecret> {
    let rest = value.strip_prefix("SCRAM-SHA-256$")?;
    let (iterations, rest) = rest.split_once(':')?;
    if iterations.is_empty() || !iterations.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let normalized_iterations = iterations.trim_start_matches('0');
    let iterations = if normalized_iterations.is_empty() {
        0
    } else {
        normalized_iterations.parse::<u32>().ok()?
    };
    if iterations == 0 {
        return None;
    }
    let (salt, keys) = rest.split_once('$')?;
    let (stored_key, server_key) = keys.split_once(':')?;
    let salt = BASE64_STANDARD.decode(salt).ok()?;
    let stored_key = BASE64_STANDARD.decode(stored_key).ok()?;
    let server_key = BASE64_STANDARD.decode(server_key).ok()?;
    if salt.is_empty() || stored_key.len() != 32 || server_key.len() != 32 {
        return None;
    }
    Some(ScramSecret {
        iterations,
        salt,
        stored_key,
        server_key,
    })
}

fn build_scram_secret(password: &str, iterations: u32) -> String {
    let mut salt = [0_u8; 16];
    rand::thread_rng().fill_bytes(&mut salt);
    let (stored_key, server_key) = scram_keys(password, &salt, iterations);
    format!(
        "SCRAM-SHA-256${iterations}:{}${}:{}",
        BASE64_STANDARD.encode(salt),
        BASE64_STANDARD.encode(stored_key),
        BASE64_STANDARD.encode(server_key)
    )
}

fn scram_secret_matches_password(secret: &ScramSecret, password: &str) -> bool {
    let (stored_key, server_key) = scram_keys(password, &secret.salt, secret.iterations);
    stored_key.as_slice() == secret.stored_key.as_slice()
        && server_key.as_slice() == secret.server_key.as_slice()
}

fn scram_keys(password: &str, salt: &[u8], iterations: u32) -> ([u8; 32], [u8; 32]) {
    let mut salted_password = [0_u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted_password);
    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let stored_key = Sha256::digest(client_key);
    let server_key = hmac_sha256(&salted_password, b"Server Key");
    (stored_key.into(), server_key)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

pub fn parse_createrole_self_grant(raw: &str) -> Result<Option<CreateRoleSelfGrant>, ParseError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let mut inherit = false;
    let mut set = false;
    for token in trimmed.split(',') {
        match token.trim().to_ascii_lowercase().as_str() {
            "" => {}
            "inherit" => inherit = true,
            "set" => set = true,
            other => {
                return Err(role_management_error(format!(
                    "invalid createrole_self_grant option: {other}"
                )));
            }
        }
    }

    Ok(Some(CreateRoleSelfGrant { inherit, set }))
}

pub fn grant_membership_authorized(
    auth: &AuthState,
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, ParseError> {
    grant_membership_authorized_with_detail(auth, catalog, role_name).map_err(|err| match err {
        GrantMembershipAuthorizationError::Parse(err) => err,
        GrantMembershipAuthorizationError::PermissionDenied { role_name, .. } => {
            role_management_error(format!("permission denied to grant role \"{role_name}\""))
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantMembershipAuthorizationError {
    Parse(ParseError),
    PermissionDenied {
        role_name: String,
        detail: Option<String>,
    },
}

pub fn grant_membership_authorized_with_detail(
    auth: &AuthState,
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, GrantMembershipAuthorizationError> {
    let role = find_role_by_name(catalog.roles(), role_name)
        .cloned()
        .ok_or_else(|| {
            GrantMembershipAuthorizationError::Parse(role_management_error(format!(
                "role \"{role_name}\" does not exist"
            )))
        })?;
    if role.oid == PG_DATABASE_OWNER_OID {
        return Err(GrantMembershipAuthorizationError::Parse(
            role_management_error(format!(
                "role \"{}\" cannot have explicit members",
                role.rolname
            )),
        ));
    }
    if role.rolsuper {
        let current = catalog
            .role_by_oid(auth.current_user_oid())
            .ok_or_else(|| {
                GrantMembershipAuthorizationError::Parse(role_management_error(
                    "permission denied to grant role",
                ))
            })?;
        if !current.rolsuper {
            return Err(GrantMembershipAuthorizationError::PermissionDenied {
                role_name: role.rolname.clone(),
                detail: Some(
                    "Only roles with the SUPERUSER attribute may grant roles with the SUPERUSER attribute.".into(),
                ),
            });
        }
        return Ok(role);
    }
    if !auth.has_admin_option(role.oid, catalog) {
        return Err(GrantMembershipAuthorizationError::PermissionDenied {
            role_name: role.rolname.clone(),
            detail: Some(format!(
                "Only roles with the ADMIN option on role \"{}\" may grant this role.",
                role.rolname
            )),
        });
    }
    Ok(role)
}

pub fn membership_row(
    roleid: u32,
    member: u32,
    grantor: u32,
    admin_option: bool,
    inherit_option: bool,
    set_option: bool,
) -> NewRoleMembership {
    NewRoleMembership {
        roleid,
        member,
        grantor,
        admin_option,
        inherit_option,
        set_option,
    }
}

fn apply_role_options(
    attrs: &mut RoleAttributes,
    options: &[RoleOption],
    add_role_to: &mut Vec<String>,
    role_members: &mut Vec<String>,
    admin_members: &mut Vec<String>,
    role_name: &str,
    password_settings: PasswordSettings,
) -> Result<bool, ParseError> {
    let mut saw_sysid = false;
    for option in options {
        match option {
            RoleOption::Superuser(enabled) => attrs.rolsuper = *enabled,
            RoleOption::CreateDb(enabled) => attrs.rolcreatedb = *enabled,
            RoleOption::CreateRole(enabled) => attrs.rolcreaterole = *enabled,
            RoleOption::Inherit(enabled) => attrs.rolinherit = *enabled,
            RoleOption::Login(enabled) => attrs.rolcanlogin = *enabled,
            RoleOption::Replication(enabled) => attrs.rolreplication = *enabled,
            RoleOption::BypassRls(enabled) => attrs.rolbypassrls = *enabled,
            RoleOption::ConnectionLimit(limit) => attrs.rolconnlimit = *limit,
            RoleOption::Password(password) => {
                attrs.rolpassword =
                    normalize_role_password(password.as_deref(), role_name, password_settings)?;
            }
            RoleOption::EncryptedPassword(password) => {
                attrs.rolpassword =
                    normalize_role_password(Some(password), role_name, password_settings)?;
            }
            RoleOption::InRole(names) => add_role_to.extend(names.iter().cloned()),
            RoleOption::Role(names) => role_members.extend(names.iter().cloned()),
            RoleOption::Admin(names) => admin_members.extend(names.iter().cloned()),
            RoleOption::Sysid(_) => {
                // :HACK: PostgreSQL emits a NOTICE here. The parser keeps SYSID accepted as a
                // backwards-compatible noise word, but notice plumbing is deferred.
                saw_sysid = true;
            }
        }
    }
    Ok(saw_sysid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, PgAuthMembersRow};

    fn role(oid: u32, name: &str) -> PgAuthIdRow {
        PgAuthIdRow {
            oid,
            rolname: name.into(),
            rolsuper: false,
            rolinherit: true,
            rolcreaterole: false,
            rolcreatedb: false,
            rolcanlogin: false,
            rolreplication: false,
            rolbypassrls: false,
            rolconnlimit: -1,
            rolpassword: None,
            rolvaliduntil: None,
        }
    }

    #[test]
    fn create_user_implies_login() {
        let spec = build_create_role_spec(
            &CreateRoleStatement {
                role_name: "app_user".into(),
                is_user: true,
                options: vec![],
            },
            PasswordSettings::default(),
        )
        .unwrap();
        assert!(spec.attrs.rolcanlogin);
    }

    #[test]
    fn membership_options_are_collected() {
        let spec = build_create_role_spec(
            &CreateRoleStatement {
                role_name: "app_user".into(),
                is_user: false,
                options: vec![
                    RoleOption::InRole(vec!["parent".into()]),
                    RoleOption::Role(vec!["member".into()]),
                    RoleOption::Admin(vec!["admin".into()]),
                ],
            },
            PasswordSettings::default(),
        )
        .unwrap();
        assert_eq!(spec.add_role_to, vec!["parent"]);
        assert_eq!(spec.role_members, vec!["member"]);
        assert_eq!(spec.admin_members, vec!["admin"]);
    }

    #[test]
    fn rename_requires_createrole_and_admin_option() {
        let mut creator = role(BOOTSTRAP_SUPERUSER_OID + 1, "creator");
        creator.rolcreaterole = true;
        let target = role(BOOTSTRAP_SUPERUSER_OID + 2, "tenant");
        let catalog = AuthCatalog::new(
            vec![
                role(BOOTSTRAP_SUPERUSER_OID, "postgres"),
                creator.clone(),
                target.clone(),
            ],
            vec![PgAuthMembersRow {
                oid: 1,
                roleid: target.oid,
                member: creator.oid,
                grantor: BOOTSTRAP_SUPERUSER_OID,
                admin_option: true,
                inherit_option: true,
                set_option: true,
            }],
        );
        let mut auth = AuthState::default();
        auth.set_session_authorization(creator.oid);

        assert!(can_rename_role(&auth, target.oid, &catalog));
    }

    #[test]
    fn parse_createrole_self_grant_values() {
        assert_eq!(
            parse_createrole_self_grant("set, inherit").unwrap(),
            Some(CreateRoleSelfGrant {
                inherit: true,
                set: true,
            })
        );
        assert_eq!(parse_createrole_self_grant("").unwrap(), None);
        assert!(parse_createrole_self_grant("bogus").is_err());
    }

    #[test]
    fn grant_membership_authorization_checks_superuser_and_admin() {
        let mut creator = role(11, "creator");
        creator.rolcreaterole = true;
        let mut super_role = role(12, "super_role");
        super_role.rolsuper = true;
        let tenant = role(13, "tenant");
        let catalog = AuthCatalog::new(
            vec![
                role(BOOTSTRAP_SUPERUSER_OID, "postgres"),
                creator.clone(),
                super_role,
                tenant.clone(),
            ],
            vec![PgAuthMembersRow {
                oid: 1,
                roleid: tenant.oid,
                member: creator.oid,
                grantor: BOOTSTRAP_SUPERUSER_OID,
                admin_option: true,
                inherit_option: false,
                set_option: true,
            }],
        );
        let mut auth = AuthState::default();
        auth.set_session_authorization(creator.oid);
        assert!(grant_membership_authorized(&auth, &catalog, "tenant").is_ok());
        assert!(grant_membership_authorized(&auth, &catalog, "super_role").is_err());
    }
}
