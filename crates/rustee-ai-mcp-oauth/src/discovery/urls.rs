//! Well-known metadata URL selection under the trusted URL budget.

use url::Url;

use crate::{McpOAuthError, config::valid_resource_url};

use super::challenge::resource_metadata_url;

pub(crate) fn resource_metadata_urls(
    resource: &Url,
    www_authenticate: Option<&str>,
) -> Result<Vec<Url>, McpOAuthError> {
    if !valid_resource_url(resource) {
        return Err(McpOAuthError::InvalidMetadata);
    }
    if let Some(header) = www_authenticate
        && let Some(url) = resource_metadata_url(header)?
    {
        return validated_metadata_urls(vec![url]);
    }
    let path = resource.path().trim_start_matches('/');
    let mut path_specific = resource.clone();
    path_specific.set_path(&format!("/.well-known/oauth-protected-resource/{path}"));
    path_specific.set_query(None);
    path_specific.set_fragment(None);
    let mut root = resource.clone();
    root.set_path("/.well-known/oauth-protected-resource");
    root.set_query(None);
    root.set_fragment(None);
    validated_metadata_urls(
        (path_specific != root)
            .then_some(vec![path_specific, root.clone()])
            .unwrap_or_else(|| vec![root]),
    )
}

pub(crate) fn authorization_server_metadata_urls(issuer: &Url) -> Result<Vec<Url>, McpOAuthError> {
    if !valid_resource_url(issuer) {
        return Err(McpOAuthError::InvalidMetadata);
    }
    let path = issuer.path().trim_matches('/');
    let oauth_path = if path.is_empty() {
        "/.well-known/oauth-authorization-server".to_owned()
    } else {
        format!("/.well-known/oauth-authorization-server/{path}")
    };
    let mut oauth = issuer.clone();
    oauth.set_path(&oauth_path);
    oauth.set_query(None);
    oauth.set_fragment(None);
    let oidc_inserted_path = if path.is_empty() {
        "/.well-known/openid-configuration".to_owned()
    } else {
        format!("/.well-known/openid-configuration/{path}")
    };
    let mut oidc_inserted = issuer.clone();
    oidc_inserted.set_path(&oidc_inserted_path);
    oidc_inserted.set_query(None);
    oidc_inserted.set_fragment(None);
    if path.is_empty() {
        return validated_metadata_urls(vec![oauth, oidc_inserted]);
    }
    let mut oidc_appended = issuer.clone();
    oidc_appended.set_path(&format!("/{path}/.well-known/openid-configuration"));
    oidc_appended.set_query(None);
    oidc_appended.set_fragment(None);
    validated_metadata_urls(vec![oauth, oidc_inserted, oidc_appended])
}

fn validated_metadata_urls(urls: Vec<Url>) -> Result<Vec<Url>, McpOAuthError> {
    urls.iter()
        .all(valid_resource_url)
        .then_some(urls)
        .ok_or(McpOAuthError::InvalidMetadata)
}
