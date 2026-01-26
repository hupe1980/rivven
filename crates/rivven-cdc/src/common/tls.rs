//! TLS configuration for CDC connections
//!
//! Provides TLS support for database connections to secure data in transit.
//! Uses rustls for modern, memory-safe TLS implementation.

use std::path::PathBuf;

/// TLS mode for database connections
#[derive(Debug, Clone, Default)]
pub enum SslMode {
    /// No TLS - plain TCP connection
    #[default]
    Disable,
    /// Try TLS, but allow unencrypted if server doesn't support it
    Prefer,
    /// Require TLS, verify server certificate against root CAs
    Require,
    /// Require TLS, verify server certificate against specified CA
    VerifyCa,
    /// Require TLS, verify both CA and server hostname
    VerifyFull,
}

impl std::fmt::Display for SslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SslMode::Disable => write!(f, "disable"),
            SslMode::Prefer => write!(f, "prefer"),
            SslMode::Require => write!(f, "require"),
            SslMode::VerifyCa => write!(f, "verify-ca"),
            SslMode::VerifyFull => write!(f, "verify-full"),
        }
    }
}

impl std::str::FromStr for SslMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disable" | "off" | "no" | "false" | "0" => Ok(SslMode::Disable),
            "prefer" => Ok(SslMode::Prefer),
            "require" => Ok(SslMode::Require),
            "verify-ca" | "verify_ca" => Ok(SslMode::VerifyCa),
            "verify-full" | "verify_full" => Ok(SslMode::VerifyFull),
            _ => Err(format!(
                "Invalid SSL mode '{}'. Valid values: disable, prefer, require, verify-ca, verify-full",
                s
            )),
        }
    }
}

/// TLS configuration for database connections
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// SSL mode (disable, prefer, require, verify-ca, verify-full)
    pub mode: SslMode,

    /// Path to CA certificate file (PEM format)
    /// Required for verify-ca and verify-full modes
    pub ca_cert_path: Option<PathBuf>,

    /// Path to client certificate file (PEM format)
    /// Used for client certificate authentication (mTLS)
    pub client_cert_path: Option<PathBuf>,

    /// Path to client private key file (PEM format)
    /// Required if client_cert_path is specified
    pub client_key_path: Option<PathBuf>,

    /// Server hostname for SNI (Server Name Indication)
    /// Defaults to the connection hostname
    pub server_name: Option<String>,

    /// Accept invalid/self-signed certificates (DANGEROUS - testing only)
    pub accept_invalid_certs: bool,

    /// Accept invalid hostnames (DANGEROUS - testing only)
    pub accept_invalid_hostnames: bool,
}

impl TlsConfig {
    /// Create a TLS config with the given mode
    pub fn new(mode: SslMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }

    /// Create TLS config that requires verification
    pub fn verify_full(ca_cert_path: PathBuf) -> Self {
        Self {
            mode: SslMode::VerifyFull,
            ca_cert_path: Some(ca_cert_path),
            ..Default::default()
        }
    }

    /// Create TLS config with client certificate authentication (mTLS)
    pub fn with_client_cert(
        ca_cert_path: PathBuf,
        client_cert_path: PathBuf,
        client_key_path: PathBuf,
    ) -> Self {
        Self {
            mode: SslMode::VerifyFull,
            ca_cert_path: Some(ca_cert_path),
            client_cert_path: Some(client_cert_path),
            client_key_path: Some(client_key_path),
            ..Default::default()
        }
    }

    /// Check if TLS is enabled
    pub fn is_enabled(&self) -> bool {
        !matches!(self.mode, SslMode::Disable)
    }

    /// Check if TLS is required (not optional)
    pub fn is_required(&self) -> bool {
        matches!(
            self.mode,
            SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull
        )
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        match self.mode {
            SslMode::VerifyCa | SslMode::VerifyFull => {
                if self.ca_cert_path.is_none() && !self.accept_invalid_certs {
                    return Err(format!(
                        "CA certificate path required for SSL mode '{}'",
                        self.mode
                    ));
                }
            }
            _ => {}
        }

        // If client cert is provided, key must also be provided
        if self.client_cert_path.is_some() && self.client_key_path.is_none() {
            return Err(
                "Client key path required when client certificate is specified".to_string(),
            );
        }

        Ok(())
    }
}

/// Build a rustls ClientConfig from TlsConfig
#[cfg(feature = "postgres-tls")]
pub fn build_rustls_config(config: &TlsConfig) -> anyhow::Result<rustls::ClientConfig> {
    use rustls::pki_types::CertificateDer;
    use std::io::BufReader;

    // Create root certificate store
    let mut root_store = rustls::RootCertStore::empty();

    // Add custom CA certificate if specified
    if let Some(ca_path) = &config.ca_cert_path {
        let ca_file = std::fs::File::open(ca_path)
            .map_err(|e| anyhow::anyhow!("Failed to open CA cert file: {}", e))?;
        let mut reader = BufReader::new(ca_file);
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("Failed to parse CA certs: {}", e))?;

        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| anyhow::anyhow!("Failed to add CA cert: {}", e))?;
        }
    } else if !config.accept_invalid_certs {
        // Use system root certificates
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    // Build client config builder
    let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

    // Add client certificate if specified (mTLS)
    let client_config = if let (Some(cert_path), Some(key_path)) =
        (&config.client_cert_path, &config.client_key_path)
    {
        // Load client certificate
        let cert_file = std::fs::File::open(cert_path)
            .map_err(|e| anyhow::anyhow!("Failed to open client cert: {}", e))?;
        let mut cert_reader = BufReader::new(cert_file);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("Failed to parse client certs: {}", e))?;

        // Load client private key
        let key_file = std::fs::File::open(key_path)
            .map_err(|e| anyhow::anyhow!("Failed to open client key: {}", e))?;
        let mut key_reader = BufReader::new(key_file);
        let key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|e| anyhow::anyhow!("Failed to parse client key: {}", e))?
            .ok_or_else(|| anyhow::anyhow!("No private key found in file"))?;

        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| anyhow::anyhow!("Failed to set client auth: {}", e))?
    } else {
        builder.with_no_client_auth()
    };

    Ok(client_config)
}

/// Create a MakeRustlsConnect for tokio-postgres-rustls
#[cfg(feature = "postgres-tls")]
pub fn make_tls_connector(
    config: &TlsConfig,
) -> anyhow::Result<tokio_postgres_rustls::MakeRustlsConnect> {
    let client_config = build_rustls_config(config)?;
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(client_config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssl_mode_parsing() {
        assert!(matches!(
            "disable".parse::<SslMode>().unwrap(),
            SslMode::Disable
        ));
        assert!(matches!(
            "prefer".parse::<SslMode>().unwrap(),
            SslMode::Prefer
        ));
        assert!(matches!(
            "require".parse::<SslMode>().unwrap(),
            SslMode::Require
        ));
        assert!(matches!(
            "verify-ca".parse::<SslMode>().unwrap(),
            SslMode::VerifyCa
        ));
        assert!(matches!(
            "verify-full".parse::<SslMode>().unwrap(),
            SslMode::VerifyFull
        ));
        assert!(matches!(
            "REQUIRE".parse::<SslMode>().unwrap(),
            SslMode::Require
        ));
        assert!("invalid".parse::<SslMode>().is_err());
    }

    #[test]
    fn test_tls_config_validation() {
        // Disable mode doesn't need CA
        let config = TlsConfig::new(SslMode::Disable);
        assert!(config.validate().is_ok());

        // Require mode doesn't need CA (just encryption)
        let config = TlsConfig::new(SslMode::Require);
        assert!(config.validate().is_ok());

        // VerifyFull needs CA (unless accept_invalid_certs)
        let config = TlsConfig::new(SslMode::VerifyFull);
        assert!(config.validate().is_err());

        // VerifyFull with accept_invalid_certs is OK
        let mut config = TlsConfig::new(SslMode::VerifyFull);
        config.accept_invalid_certs = true;
        assert!(config.validate().is_ok());

        // Client cert needs key
        let mut config = TlsConfig::new(SslMode::Require);
        config.client_cert_path = Some(PathBuf::from("/path/to/cert.pem"));
        assert!(config.validate().is_err());

        config.client_key_path = Some(PathBuf::from("/path/to/key.pem"));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_tls_config_helpers() {
        let config = TlsConfig::new(SslMode::Disable);
        assert!(!config.is_enabled());
        assert!(!config.is_required());

        let config = TlsConfig::new(SslMode::Prefer);
        assert!(config.is_enabled());
        assert!(!config.is_required());

        let config = TlsConfig::new(SslMode::Require);
        assert!(config.is_enabled());
        assert!(config.is_required());

        let config = TlsConfig::new(SslMode::VerifyFull);
        assert!(config.is_enabled());
        assert!(config.is_required());
    }

    #[test]
    fn test_ssl_mode_display() {
        assert_eq!(SslMode::Disable.to_string(), "disable");
        assert_eq!(SslMode::VerifyFull.to_string(), "verify-full");
    }
}
