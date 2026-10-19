use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IpCidr {
    network: IpAddr,
    prefix: u8,
}

impl FromStr for IpCidr {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (address, prefix) = value
            .split_once('/')
            .ok_or_else(|| "CIDR must include a prefix length".to_owned())?;
        let network = address
            .parse::<IpAddr>()
            .map_err(|_| "CIDR address must be a numeric IPv4 or IPv6 address".to_owned())?;
        let prefix = prefix
            .parse::<u8>()
            .map_err(|_| "CIDR prefix must be numeric".to_owned())?;
        let max = if network.is_ipv4() { 32 } else { 128 };
        if prefix > max {
            return Err(format!("CIDR prefix must be at most {max}"));
        }
        Ok(Self { network, prefix })
    }
}

impl IpCidr {
    pub fn contains(&self, address: IpAddr) -> bool {
        match (self.network, address) {
            (IpAddr::V4(network), IpAddr::V4(address)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - self.prefix)
                };
                u32::from(network) & mask == u32::from(address) & mask
            }
            (IpAddr::V6(network), IpAddr::V6(address)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - self.prefix)
                };
                u128::from(network) & mask == u128::from(address) & mask
            }
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientIpSource {
    DirectPeer,
    TrustedForwarded,
    TrustedProxyMissing,
    TrustedProxyInvalid,
    NoPeer,
}

impl ClientIpSource {
    pub fn label(self) -> &'static str {
        match self {
            Self::DirectPeer => "direct_peer",
            Self::TrustedForwarded => "trusted_forwarded",
            Self::TrustedProxyMissing => "trusted_proxy_missing",
            Self::TrustedProxyInvalid => "trusted_proxy_invalid",
            Self::NoPeer => "no_peer",
        }
    }
}

pub fn resolve_client_ip(
    peer: Option<SocketAddr>,
    forwarded_for: Option<&str>,
    trusted_proxies: &[IpCidr],
) -> (Option<IpAddr>, ClientIpSource) {
    let Some(peer) = peer else {
        return (None, ClientIpSource::NoPeer);
    };
    let peer_ip = peer.ip();
    if !trusted_proxies.iter().any(|cidr| cidr.contains(peer_ip)) {
        return (Some(peer_ip), ClientIpSource::DirectPeer);
    }
    let Some(chain) = forwarded_for else {
        return (None, ClientIpSource::TrustedProxyMissing);
    };
    if chain.len() > 512 {
        return (None, ClientIpSource::TrustedProxyInvalid);
    }
    let addresses: Result<Vec<IpAddr>, _> = chain
        .split(',')
        .map(|part| part.trim().parse::<IpAddr>())
        .collect();
    let Ok(addresses) = addresses else {
        return (None, ClientIpSource::TrustedProxyInvalid);
    };
    if addresses.is_empty() || addresses.len() > 8 {
        return (None, ClientIpSource::TrustedProxyInvalid);
    }
    let mut current = peer_ip;
    for address in addresses.into_iter().rev() {
        if !trusted_proxies.iter().any(|cidr| cidr.contains(current)) {
            break;
        }
        current = address;
    }
    (Some(current), ClientIpSource::TrustedForwarded)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cidr(value: &str) -> IpCidr {
        value.parse().unwrap()
    }

    #[test]
    fn ignores_forwarded_headers_from_untrusted_peers() {
        let trusted = [cidr("192.0.2.0/24")];
        let (address, source) = resolve_client_ip(
            Some("198.51.100.7:443".parse().unwrap()),
            Some("203.0.113.9"),
            &trusted,
        );
        assert_eq!(address, Some("198.51.100.7".parse().unwrap()));
        assert_eq!(source, ClientIpSource::DirectPeer);
    }

    #[test]
    fn follows_only_trusted_proxy_hops_and_rejects_malformed_chains() {
        let trusted = [cidr("192.0.2.0/24")];
        let (address, source) = resolve_client_ip(
            Some("192.0.2.10:443".parse().unwrap()),
            Some("203.0.113.9, 192.0.2.11"),
            &trusted,
        );
        assert_eq!(address, Some("203.0.113.9".parse().unwrap()));
        assert_eq!(source, ClientIpSource::TrustedForwarded);
        let (address, source) = resolve_client_ip(
            Some("192.0.2.10:443".parse().unwrap()),
            Some("not-an-ip"),
            &trusted,
        );
        assert_eq!(address, None);
        assert_eq!(source, ClientIpSource::TrustedProxyInvalid);
    }
}
