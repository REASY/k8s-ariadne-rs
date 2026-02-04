use std::sync::Once;

static RUSTLS_PROVIDER_ONCE: Once = Once::new();

pub(crate) fn install_rustls_provider() {
    RUSTLS_PROVIDER_ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_rustls_provider_is_idempotent() {
        install_rustls_provider();
        install_rustls_provider();
    }
}
