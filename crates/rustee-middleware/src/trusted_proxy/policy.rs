//! Trusted reverse-proxy network admission and topology configuration.

use std::{fmt, net::IpAddr};

pub(super) const MAX_FORWARDED_CHAIN_HOPS: usize = 16;
/// Maximum number of distinct trusted proxy networks accepted by one policy.
pub const MAX_TRUSTED_PROXY_NETWORKS: usize = 64;

/// One IP network trusted to terminate or forward traffic to this application.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct TrustedProxyNetwork {
    network: IpAddr,
    prefix_length: u8,
}

impl fmt::Debug for TrustedProxyNetwork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let address_family = match self.network {
            IpAddr::V4(_) => "ipv4",
            IpAddr::V6(_) => "ipv6",
        };
        formatter
            .debug_struct("TrustedProxyNetwork")
            .field("address_family", &address_family)
            .field("prefix_length", &self.prefix_length)
            .finish()
    }
}

impl TrustedProxyNetwork {
    /// Creates a normalized IPv4 or IPv6 CIDR network.
    ///
    /// # Errors
    ///
    /// Returns [`TrustedProxyError::InvalidPrefixLength`] when the prefix does not fit the IP
    /// family. A `/0` network is deliberately rejected because it would trust every peer.
    pub fn new(network: IpAddr, prefix_length: u8) -> Result<Self, TrustedProxyError> {
        let width = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix_length == 0 || prefix_length > width {
            return Err(TrustedProxyError::InvalidPrefixLength);
        }
        Ok(Self {
            network: normalize_ip(network, prefix_length),
            prefix_length,
        })
    }

    fn contains(self, address: IpAddr) -> bool {
        match (self.network, address) {
            (IpAddr::V4(network), IpAddr::V4(address)) => {
                let shift = 32 - u32::from(self.prefix_length);
                u32::from(network) >> shift == u32::from(address) >> shift
            }
            (IpAddr::V6(network), IpAddr::V6(address)) => {
                let shift = 128 - u32::from(self.prefix_length);
                u128::from(network) >> shift == u128::from(address) >> shift
            }
            _ => false,
        }
    }
}

/// Explicit policy for one or more trusted reverse-proxy networks.
#[derive(Clone, Eq, PartialEq)]
pub struct TrustedProxyPolicy {
    networks: Vec<TrustedProxyNetwork>,
    forwarded_chain_hops: usize,
}

impl TrustedProxyPolicy {
    /// Creates a policy that trusts the supplied non-empty network allowlist.
    ///
    /// # Errors
    ///
    /// Returns [`TrustedProxyError::EmptyNetworkAllowlist`] when no proxy networks are supplied
    /// or [`TrustedProxyError::NetworkAllowlistLimit`] when more than
    /// [`MAX_TRUSTED_PROXY_NETWORKS`] distinct networks are supplied.
    pub fn new(
        networks: impl IntoIterator<Item = TrustedProxyNetwork>,
    ) -> Result<Self, TrustedProxyError> {
        let mut collected = Vec::with_capacity(MAX_TRUSTED_PROXY_NETWORKS);
        for network in networks {
            if collected.contains(&network) {
                continue;
            }
            if collected.len() == MAX_TRUSTED_PROXY_NETWORKS {
                return Err(TrustedProxyError::NetworkAllowlistLimit);
            }
            collected.push(network);
        }
        let networks = collected;
        if networks.is_empty() {
            return Err(TrustedProxyError::EmptyNetworkAllowlist);
        }
        Ok(Self {
            networks,
            forwarded_chain_hops: 0,
        })
    }

    /// Allows up to `hops` trusted intermediary `Forwarded` elements before the client address.
    ///
    /// The direct transport peer must still match this policy. The default is zero, preserving the
    /// one-element single-hop contract. Earlier chain elements cannot supply `proto` or `host`;
    /// those values must be asserted by the direct trusted proxy in the rightmost element.
    ///
    /// # Errors
    ///
    /// Returns [`TrustedProxyError::InvalidForwardedChainHops`] when `hops` is zero or exceeds the
    /// bounded parser limit.
    pub fn with_forwarded_chain_hops(mut self, hops: usize) -> Result<Self, TrustedProxyError> {
        if hops == 0 || hops > MAX_FORWARDED_CHAIN_HOPS {
            return Err(TrustedProxyError::InvalidForwardedChainHops);
        }
        self.forwarded_chain_hops = hops;
        Ok(self)
    }

    pub(super) fn trusts(&self, address: IpAddr) -> bool {
        self.networks
            .iter()
            .copied()
            .any(|network| network.contains(address))
    }

    pub(super) const fn forwarded_chain_hops(&self) -> usize {
        self.forwarded_chain_hops
    }
}

impl fmt::Debug for TrustedProxyPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrustedProxyPolicy")
            .field("trusted_network_count", &self.networks.len())
            .field("forwarded_chain_hops", &self.forwarded_chain_hops)
            .finish()
    }
}

/// Invalid trusted-proxy configuration or forwarded-header input.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TrustedProxyError {
    /// A CIDR prefix was zero or exceeded its address family width.
    #[error("trusted proxy network prefix must be within its IP family and not /0")]
    InvalidPrefixLength,
    /// No trusted networks were configured.
    #[error("trusted proxy policy requires at least one network")]
    EmptyNetworkAllowlist,
    /// The trusted-proxy network list exceeded its fixed safety bound.
    #[error("trusted proxy network allowlist exceeds the bounded limit")]
    NetworkAllowlistLimit,
    /// The configured forwarded chain depth was zero or exceeded the bounded parser limit.
    #[error("trusted proxy forwarded chain hops must be between 1 and 16")]
    InvalidForwardedChainHops,
}

fn normalize_ip(address: IpAddr, prefix: u8) -> IpAddr {
    match address {
        IpAddr::V4(address) => {
            let shift = 32 - u32::from(prefix);
            IpAddr::V4((u32::from(address) >> shift << shift).into())
        }
        IpAddr::V6(address) => {
            let shift = 128 - u32::from(prefix);
            IpAddr::V6((u128::from(address) >> shift << shift).into())
        }
    }
}
