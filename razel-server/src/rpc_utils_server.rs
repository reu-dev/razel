use anyhow::{anyhow, bail, Context, Result};
use quinn::crypto::rustls::QuicServerConfig;
use quinn::rustls::pki_types::pem::PemObject;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use quinn::{rustls, Endpoint};
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

pub fn new_server_endpoint(
    listen: SocketAddr,
    cert_path: Option<PathBuf>,
    key_path: Option<PathBuf>,
) -> Result<Endpoint> {
    let (certs, key) = match (cert_path, key_path) {
        (Some(cert), Some(key)) => get_cert_from_custom_files(cert, key)?,
        (None, None) => create_cert_in_default_dir()?,
        _ => unreachable!(),
    };
    let server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs.clone(), key)?;
    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.max_concurrent_uni_streams(0_u8.into());
    let endpoint = Endpoint::server(server_config, listen)?;
    Ok(endpoint)
}

fn get_cert_from_custom_files(
    cert_path: PathBuf,
    key_path: PathBuf,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let key = if key_path.extension().is_some_and(|x| x == "der") {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            fs::read(key_path).context("failed to read private key file")?,
        ))
    } else {
        PrivateKeyDer::from_pem_file(key_path)
            .context("failed to read PEM from private key file")?
    };
    let cert_chain = if cert_path.extension().is_some_and(|x| x == "der") {
        vec![CertificateDer::from(
            fs::read(cert_path).context("failed to read certificate chain file")?,
        )]
    } else {
        CertificateDer::pem_file_iter(cert_path)
            .context("failed to read PEM from certificate chain file")?
            .collect::<Result<_, _>>()
            .context("invalid PEM-encoded certificate")?
    };
    Ok((cert_chain, key))
}

fn create_cert_in_default_dir() -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let dirs = directories::ProjectDirs::from("de", "reu-dev", "razel")
        .ok_or_else(|| anyhow!("failed to get ProjectDirs"))?;
    let path = dirs.data_local_dir();
    let cert_path = path.join("cert.der");
    let key_path = path.join("key.der");
    let (cert, key) = match fs::read(&cert_path).and_then(|x| Ok((x, fs::read(&key_path)?))) {
        Ok((cert, key)) => (
            CertificateDer::from(cert),
            PrivateKeyDer::try_from(key).map_err(anyhow::Error::msg)?,
        ),
        Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!("generating self-signed certificate");
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
            let cert = cert.cert.into();
            fs::create_dir_all(path).context("failed to create certificate directory")?;
            fs::write(&cert_path, &cert).context("failed to write certificate")?;
            fs::write(&key_path, key.secret_pkcs8_der()).context("failed to write private key")?;
            (cert, key.into())
        }
        Err(e) => {
            bail!("failed to read certificate: {}", e);
        }
    };
    Ok((vec![cert], key))
}
