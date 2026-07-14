//! ECR pull-token minting for sandbox runs.
//!
//! Candidate router images live in a private ECR repository. On EKS the node
//! IAM role authorizes pulls natively and nothing here runs. Everywhere else
//! (local clusters, cross-account pulls) the dashboard needs AWS credentials
//! with `ecr:GetAuthorizationToken` + repository read permission:
//!
//!   DEJA_ECR_ACCESS_KEY_ID       access key the dashboard uses for ECR
//!   DEJA_ECR_SECRET_ACCESS_KEY   its secret
//!   DEJA_ECR_SESSION_TOKEN       optional STS session token for temporary creds
//!   DEJA_ECR_REGION              optional; parsed from the repository host
//!                                (<acct>.dkr.ecr.<region>.amazonaws.com)
//!                                when unset
//!
//! When set, the driver exchanges them for the 12-hour ECR authorization
//! token (username "AWS") before each sandbox install and passes it to the
//! chart as `registryCredentials`, which becomes a dockerconfigjson Secret
//! inside the run namespace referenced by every pod. Each run mints a fresh
//! token, so expiry never bites.

use base64::Engine;

/// An ECR registry recognized from an image repository reference.
#[derive(Debug, PartialEq, Eq)]
pub struct EcrRegistry {
    pub host: String,
    pub region: String,
}

/// Parse `<account>.dkr.ecr.<region>.amazonaws.com/...` — returns None for
/// non-ECR repositories (docker.io, ghcr, local names).
pub fn parse_ecr_registry(repository: &str) -> Option<EcrRegistry> {
    let host = repository.split('/').next()?;
    let parts: Vec<&str> = host.split('.').collect();
    if parts.len() == 6
        && parts[1] == "dkr"
        && parts[2] == "ecr"
        && parts[4] == "amazonaws"
        && parts[5] == "com"
    {
        Some(EcrRegistry {
            host: host.to_owned(),
            region: parts[3].to_owned(),
        })
    } else {
        None
    }
}

/// Exchange static AWS credentials for an ECR registry password (the
/// dockerconfigjson password half; username is always "AWS").
pub fn mint_token(
    region: &str,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
) -> Result<String, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("ecr: tokio runtime: {e}"))?;
    let region = region.to_owned();
    let access_key_id = access_key_id.to_owned();
    let secret_access_key = secret_access_key.to_owned();
    let session_token = session_token.trim().to_owned();
    runtime.block_on(async move {
        let credentials = aws_sdk_ecr::config::Credentials::new(
            access_key_id,
            secret_access_key,
            if session_token.is_empty() {
                None
            } else {
                Some(session_token)
            },
            None,
            "deja-dashboard",
        );
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region))
            .credentials_provider(credentials)
            .load()
            .await;
        let client = aws_sdk_ecr::Client::new(&config);
        let output = client
            .get_authorization_token()
            .send()
            .await
            .map_err(|e| format!("ecr: GetAuthorizationToken: {e}"))?;
        let data = output
            .authorization_data()
            .first()
            .ok_or_else(|| "ecr: no authorization data returned".to_owned())?;
        let token_b64 = data
            .authorization_token()
            .ok_or_else(|| "ecr: authorization data has no token".to_owned())?;
        decode_registry_password(token_b64)
    })
}

/// The ECR token is base64("AWS:<password>"); extract the password half.
pub fn decode_registry_password(token_b64: &str) -> Result<String, String> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(token_b64)
        .map_err(|e| format!("ecr: token base64: {e}"))?;
    let decoded = String::from_utf8(decoded).map_err(|e| format!("ecr: token utf8: {e}"))?;
    let (user, password) = decoded
        .split_once(':')
        .ok_or_else(|| "ecr: token is not user:password".to_owned())?;
    if user != "AWS" {
        return Err(format!("ecr: unexpected token user {user:?}"));
    }
    Ok(password.to_owned())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;

    #[test]
    fn recognizes_ecr_repository_hosts() {
        let got =
            parse_ecr_registry("123456789012.dkr.ecr.us-east-1.amazonaws.com/router-candidate")
                .unwrap();
        assert_eq!(got.host, "123456789012.dkr.ecr.us-east-1.amazonaws.com");
        assert_eq!(got.region, "us-east-1");
    }

    #[test]
    fn ignores_non_ecr_repositories() {
        assert_eq!(
            parse_ecr_registry("docker.juspay.io/juspaydotin/hyperswitch-router"),
            None
        );
        assert_eq!(parse_ecr_registry("deja-replay-agent"), None);
        assert_eq!(parse_ecr_registry("ghcr.io/juspay/superposition"), None);
    }

    #[test]
    fn decodes_the_password_half_of_the_token() {
        let token = base64::engine::general_purpose::STANDARD.encode("AWS:sekret-token");
        assert_eq!(decode_registry_password(&token).unwrap(), "sekret-token");
    }

    #[test]
    fn rejects_malformed_tokens() {
        let token = base64::engine::general_purpose::STANDARD.encode("no-colon-here");
        assert!(decode_registry_password(&token).is_err());
    }
}
