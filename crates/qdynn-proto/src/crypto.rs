use crate::error::ProtoError;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

const BRIDGE_V1_DOMAIN_LABEL: &str = "tdesktop-web-proxy-bridge-v1\n";

/// Parses a hex or raw secret string into canonical secret bytes.
pub fn parse_secret(secret_str: &str) -> Result<Vec<u8>, ProtoError> {
    if secret_str != secret_str.trim() {
        return Err(ProtoError::InvalidSecret("surrounding whitespace".into()));
    }
    let bytes = hex::decode(secret_str)
        .or_else(|_| {
            URL_SAFE_NO_PAD
                .decode(secret_str)
                .or_else(|_| URL_SAFE.decode(secret_str))
        })
        .map_err(|_| ProtoError::InvalidSecret("expected hex or unpadded base64url".into()))?;
    let valid = bytes.len() == 16 || (bytes.len() == 17 && bytes[0] == 0xdd);
    if !valid {
        return Err(ProtoError::InvalidSecret(
            "expected 16 bytes or dd + 16 bytes".into(),
        ));
    }
    Ok(bytes)
}

/// Computes the 32-byte domain-separated bridge capability according to Telegram Web Proxy v1 spec:
/// capability = base64url_no_padding(HMAC-SHA256(key=S, message=UTF8("tdesktop-web-proxy-bridge-v1\n" + H)))
pub fn derive_bridge_capability(hostname: &str, secret_bytes: &[u8]) -> String {
    let canonical_host = hostname.trim().trim_end_matches('.').to_ascii_lowercase();
    let message = format!("{}{}", BRIDGE_V1_DOMAIN_LABEL, canonical_host);

    let mut mac = HmacSha256::new_from_slice(secret_bytes).expect("HMAC can accept any key length");
    mac.update(message.as_bytes());
    let result = mac.finalize().into_bytes();

    URL_SAFE_NO_PAD.encode(result)
}

/// Constant-time verification of a client's bridge capability.
pub fn verify_bridge_capability(
    hostname: &str,
    secret_bytes: &[u8],
    candidate_capability: &str,
) -> bool {
    let expected = derive_bridge_capability(hostname, secret_bytes);
    let expected_bytes = expected.as_bytes();
    let candidate_bytes = candidate_capability.as_bytes();

    if expected_bytes.len() != candidate_bytes.len() {
        return false;
    }

    expected_bytes.ct_eq(candidate_bytes).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_secret_lengths_and_prefixes_are_enforced() {
        for secret in [
            "",
            "aa",
            "aa000102030405060708090a0b0c0d0e0f",
            "ee000102030405060708090a0b0c0d0e0f746573742e636f6d",
        ] {
            assert!(parse_secret(secret).is_err());
        }
        let key = [42u8; 16];
        assert_eq!(parse_secret(&URL_SAFE_NO_PAD.encode(key)).unwrap(), key);
        assert_eq!(parse_secret(&URL_SAFE.encode(key)).unwrap(), key);
    }

    #[test]
    fn test_normative_vectors_plain_secret() {
        let host = "proxy.example.com";
        let secret = hex::decode("000102030405060708090a0b0c0d0e0f").unwrap();
        let capability = derive_bridge_capability(host, &secret);
        assert_eq!(capability, "MHLEY5PmW1GWqJkSrlmJpvJUiLhBH_QKy6yKg8a0JPk");
        assert!(verify_bridge_capability(host, &secret, &capability));
        assert!(!verify_bridge_capability(
            host,
            &secret,
            "invalid_capability"
        ));
    }

    #[test]
    fn test_normative_vectors_random_padding_dd_secret() {
        let host = "proxy.example.com";
        let secret = hex::decode("dd000102030405060708090a0b0c0d0e0f").unwrap();
        let capability = derive_bridge_capability(host, &secret);
        assert_eq!(capability, "IpJrt3e7sKtzPyoXy6w-Zj6GGEvsvclN66JzQEfPYLA");
        assert!(verify_bridge_capability(host, &secret, &capability));
    }

    #[test]
    fn test_hostname_normalization() {
        let secret = hex::decode("000102030405060708090a0b0c0d0e0f").unwrap();
        let cap1 = derive_bridge_capability("proxy.example.com", &secret);
        let cap2 = derive_bridge_capability("PROXY.EXAMPLE.COM.", &secret);
        let cap3 = derive_bridge_capability("  proxy.example.com  ", &secret);
        assert_eq!(cap1, cap2);
        assert_eq!(cap1, cap3);
    }
}
