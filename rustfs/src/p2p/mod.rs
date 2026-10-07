//! Boundary for the future decentralized networking subsystem.
//!
//! RustFS still uses HTTP and gRPC for cluster communication. Keep the
//! experimental peer-to-peer surface isolated here so the eventual hybrid
//! bootstrap path can grow without disturbing the existing transport stack.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

const MAX_NODE_NAME_LEN: usize = 253;
const MAX_PEER_ID_LEN: usize = 128;
const MAX_PEER_ADDRESS_LEN: usize = 1024;
const MIN_MULTIADDR_COMPONENTS: usize = 6;

/// Errors returned by the p2p primitive validators.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum P2pPrimitiveError {
    #[error("node deployment identity cannot be nil")]
    NilDeploymentId,
    #[error("node name is invalid")]
    InvalidNodeName,
    #[error("peer id is invalid")]
    InvalidPeerId,
    #[error("peer address is invalid")]
    InvalidPeerAddress,
    #[error("bootstrap rendezvous namespace is invalid")]
    InvalidRendezvousNamespace,
    #[error("bootstrap config requires at least one discovery source")]
    MissingBootstrapSource,
    #[error("bootstrap retry interval must be non-zero")]
    InvalidRetryInterval,
    #[error("bootstrap peer budget must be non-zero")]
    InvalidPeerBudget,
    #[error("bootstrap peers must be unique")]
    DuplicateBootstrapPeer,
    #[error("p2p config has an inconsistent enabled state")]
    InvalidConfigState,
}

/// Stable identity for one node in the cluster.
///
/// The deployment identifier keeps identities partitioned across deployments,
/// while the peer id is the libp2p-facing identity that later runtime code
/// will publish over the network.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeIdentity {
    deployment_id: Uuid,
    node_name: String,
    peer_id: PeerId,
}

impl NodeIdentity {
    pub fn new(deployment_id: Uuid, node_name: impl Into<String>, peer_id: PeerId) -> Result<Self, P2pPrimitiveError> {
        if deployment_id.is_nil() {
            return Err(P2pPrimitiveError::NilDeploymentId);
        }
        let node_name = validate_node_name(node_name.into())?;
        Ok(Self {
            deployment_id,
            node_name,
            peer_id,
        })
    }

    pub fn deployment_id(&self) -> Uuid {
        self.deployment_id
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }
}

/// Canonical libp2p peer identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PeerId(String);

impl PeerId {
    pub fn new(value: impl Into<String>) -> Result<Self, P2pPrimitiveError> {
        let value = validate_peer_id(value.into())?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for PeerId {
    type Err = P2pPrimitiveError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value.to_string())
    }
}

/// Peer reachability target expressed as a canonical multiaddr string and its
/// extracted peer id.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerAddress {
    multiaddr: String,
    peer_id: PeerId,
}

impl PeerAddress {
    pub fn new(value: impl Into<String>) -> Result<Self, P2pPrimitiveError> {
        let multiaddr = validate_peer_address(value.into())?;
        let peer_id = extract_peer_id(&multiaddr)?;
        Ok(Self { multiaddr, peer_id })
    }

    pub fn as_str(&self) -> &str {
        &self.multiaddr
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }
}

impl fmt::Display for PeerAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.multiaddr)
    }
}

impl FromStr for PeerAddress {
    type Err = P2pPrimitiveError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value.to_string())
    }
}

/// Configuration for hybrid bootstrap discovery.
///
/// `static_peers` represent seed peers that are always eligible for dialing,
/// while an optional rendezvous namespace enables opportunistic discovery over
/// the future libp2p control plane.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct P2pConfig {
    enabled: bool,
    local_identity: Option<NodeIdentity>,
    bootstrap: Option<BootstrapConfig>,
}

impl P2pConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            local_identity: None,
            bootstrap: None,
        }
    }

    pub fn enabled(local_identity: NodeIdentity, bootstrap: BootstrapConfig) -> Self {
        Self {
            enabled: true,
            local_identity: Some(local_identity),
            bootstrap: Some(bootstrap),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn local_identity(&self) -> Option<&NodeIdentity> {
        self.local_identity.as_ref()
    }

    pub fn bootstrap(&self) -> Option<&BootstrapConfig> {
        self.bootstrap.as_ref()
    }

    pub fn validate(&self) -> Result<(), P2pPrimitiveError> {
        if self.enabled != self.local_identity.is_some() || self.enabled != self.bootstrap.is_some() {
            return Err(P2pPrimitiveError::InvalidConfigState);
        }
        Ok(())
    }

    /// Build an enabled config from raw CLI/environment values and a resolved
    /// deployment identity. Used by startup after the AppContext is published.
    pub fn from_deployment_and_config(
        deployment_id: Uuid,
        node_name: impl Into<String>,
        peer_id: impl Into<String>,
        static_peers: Vec<String>,
        rendezvous_namespace: Option<String>,
        retry_interval_secs: u64,
        max_bootstrap_peers: usize,
    ) -> Result<Self, P2pPrimitiveError> {
        let identity = NodeIdentity::new(deployment_id, node_name, PeerId::new(peer_id)?)?;
        let peers = static_peers
            .into_iter()
            .map(PeerAddress::new)
            .collect::<Result<Vec<_>, _>>()?;
        let bootstrap = BootstrapConfig::new(peers, rendezvous_namespace, retry_interval_secs, max_bootstrap_peers)?;
        Ok(Self::enabled(identity, bootstrap))
    }
}

impl Default for P2pConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapConfig {
    static_peers: Vec<PeerAddress>,
    rendezvous_namespace: Option<String>,
    retry_interval_secs: u64,
    max_bootstrap_peers: usize,
}

impl BootstrapConfig {
    pub fn new(
        static_peers: Vec<PeerAddress>,
        rendezvous_namespace: Option<String>,
        retry_interval_secs: u64,
        max_bootstrap_peers: usize,
    ) -> Result<Self, P2pPrimitiveError> {
        if retry_interval_secs == 0 {
            return Err(P2pPrimitiveError::InvalidRetryInterval);
        }
        if max_bootstrap_peers == 0 {
            return Err(P2pPrimitiveError::InvalidPeerBudget);
        }

        let rendezvous_namespace = match rendezvous_namespace {
            Some(namespace) => Some(validate_rendezvous_namespace(namespace)?),
            None => None,
        };

        if static_peers.is_empty() && rendezvous_namespace.is_none() {
            return Err(P2pPrimitiveError::MissingBootstrapSource);
        }
        if static_peers.len() > max_bootstrap_peers {
            return Err(P2pPrimitiveError::InvalidPeerBudget);
        }

        let mut seen = HashSet::with_capacity(static_peers.len());
        if static_peers.iter().any(|peer| !seen.insert(peer.as_str().to_owned())) {
            return Err(P2pPrimitiveError::DuplicateBootstrapPeer);
        }

        Ok(Self {
            static_peers,
            rendezvous_namespace,
            retry_interval_secs,
            max_bootstrap_peers,
        })
    }

    pub fn static_peers(&self) -> &[PeerAddress] {
        &self.static_peers
    }

    pub fn rendezvous_namespace(&self) -> Option<&str> {
        self.rendezvous_namespace.as_deref()
    }

    pub fn retry_interval_secs(&self) -> u64 {
        self.retry_interval_secs
    }

    pub fn max_bootstrap_peers(&self) -> usize {
        self.max_bootstrap_peers
    }
}

pub fn validate_node_name(value: String) -> Result<String, P2pPrimitiveError> {
    if value.is_empty() || value.len() > MAX_NODE_NAME_LEN || value.chars().any(|ch| ch.is_control() || ch.is_whitespace()) {
        return Err(P2pPrimitiveError::InvalidNodeName);
    }
    Ok(value)
}

pub fn validate_peer_id(value: String) -> Result<String, P2pPrimitiveError> {
    if value.is_empty()
        || value.len() > MAX_PEER_ID_LEN
        || value.chars().any(|ch| ch.is_control() || ch.is_whitespace())
        || value
            .chars()
            .any(|ch| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '=')))
    {
        return Err(P2pPrimitiveError::InvalidPeerId);
    }
    Ok(value)
}

pub fn validate_peer_address(value: String) -> Result<String, P2pPrimitiveError> {
    if value.is_empty()
        || value.len() > MAX_PEER_ADDRESS_LEN
        || !value.starts_with('/')
        || value.chars().any(|ch| ch.is_control() || ch.is_whitespace())
    {
        return Err(P2pPrimitiveError::InvalidPeerAddress);
    }

    let parts: Vec<&str> = value.split('/').skip(1).collect();
    if parts.len() < MIN_MULTIADDR_COMPONENTS || parts.len() % 2 != 0 {
        return Err(P2pPrimitiveError::InvalidPeerAddress);
    }
    if parts.chunks_exact(2).any(|pair| pair[0].is_empty() || pair[1].is_empty()) {
        return Err(P2pPrimitiveError::InvalidPeerAddress);
    }
    if parts[parts.len() - 2] != "p2p" {
        return Err(P2pPrimitiveError::InvalidPeerAddress);
    }

    validate_peer_id(parts[parts.len() - 1].to_string())?;
    Ok(value)
}

pub fn validate_rendezvous_namespace(value: String) -> Result<String, P2pPrimitiveError> {
    if value.is_empty()
        || value.len() > MAX_NODE_NAME_LEN
        || value.chars().any(|ch| ch.is_control() || ch.is_whitespace() || ch == '/')
    {
        return Err(P2pPrimitiveError::InvalidRendezvousNamespace);
    }
    Ok(value)
}

pub fn validate_bootstrap_config(config: BootstrapConfig) -> Result<BootstrapConfig, P2pPrimitiveError> {
    BootstrapConfig::new(
        config.static_peers,
        config.rendezvous_namespace,
        config.retry_interval_secs,
        config.max_bootstrap_peers,
    )
}

pub fn validate_p2p_config(config: P2pConfig) -> Result<P2pConfig, P2pPrimitiveError> {
    config.validate()?;
    Ok(config)
}

pub fn extract_peer_id(value: &str) -> Result<PeerId, P2pPrimitiveError> {
    let peer_id = value.rsplit_once("/p2p/").ok_or(P2pPrimitiveError::InvalidPeerAddress)?.1;
    PeerId::new(peer_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn sample_peer_id() -> PeerId {
        PeerId::new("12D3KooWHybridPeerIdentityExample".to_string()).expect("sample peer id")
    }

    #[test]
    fn p2p_primitives_reject_nil_deployment_id() {
        let err = NodeIdentity::new(Uuid::nil(), "node-a", sample_peer_id()).expect_err("nil deployment id should fail");
        assert_eq!(err, P2pPrimitiveError::NilDeploymentId);
    }

    #[test]
    fn p2p_primitives_construct_node_identity() {
        let deployment_id = Uuid::new_v4();
        let identity = NodeIdentity::new(deployment_id, "node-a", sample_peer_id()).expect("valid node identity");

        assert_eq!(identity.deployment_id(), deployment_id);
        assert_eq!(identity.node_name(), "node-a");
        assert_eq!(identity.peer_id().as_str(), "12D3KooWHybridPeerIdentityExample");
    }

    #[test]
    fn p2p_primitives_parse_peer_address_and_peer_id() {
        let address = PeerAddress::new("/dns4/bootstrap.example.com/tcp/4001/p2p/12D3KooWHybridPeerIdentityExample")
            .expect("valid peer address");

        assert_eq!(address.peer_id().as_str(), "12D3KooWHybridPeerIdentityExample");
        assert_eq!(
            address.as_str(),
            "/dns4/bootstrap.example.com/tcp/4001/p2p/12D3KooWHybridPeerIdentityExample"
        );
    }

    #[test]
    fn p2p_primitives_reject_addresses_without_peer_suffix() {
        let err = PeerAddress::new("/dns4/bootstrap.example.com/tcp/4001").expect_err("missing p2p suffix should fail");
        assert_eq!(err, P2pPrimitiveError::InvalidPeerAddress);
    }

    #[test]
    fn p2p_primitives_validate_bootstrap_config_sources_and_budgets() {
        let peer = PeerAddress::new("/dns4/bootstrap.example.com/tcp/4001/p2p/12D3KooWHybridPeerIdentityExample")
            .expect("valid peer address");
        let cfg = BootstrapConfig::new(vec![peer], Some("rustfs".to_string()), 5, 8).expect("valid bootstrap config");

        assert_eq!(cfg.retry_interval_secs(), 5);
        assert_eq!(cfg.max_bootstrap_peers(), 8);
        assert_eq!(cfg.rendezvous_namespace(), Some("rustfs"));
        assert_eq!(cfg.static_peers().len(), 1);
    }

    #[test]
    fn p2p_primitives_config_scaffold_tracks_enabled_state() {
        let peer = sample_peer_id();
        let identity = NodeIdentity::new(Uuid::new_v4(), "node-a", peer).expect("identity");
        let bootstrap = BootstrapConfig::new(
            vec![
                PeerAddress::new("/dns4/bootstrap.example.com/tcp/4001/p2p/12D3KooWHybridPeerIdentityExample")
                    .expect("valid peer address"),
            ],
            Some("rustfs".to_string()),
            5,
            8,
        )
        .expect("bootstrap");

        let config = P2pConfig::enabled(identity, bootstrap);
        assert!(config.is_enabled());
        assert!(config.local_identity().is_some());
        assert!(config.bootstrap().is_some());
        assert_eq!(validate_p2p_config(config).expect("valid config").is_enabled(), true);
    }

    #[test]
    fn p2p_primitives_reject_duplicate_bootstrap_peers() {
        let peer = PeerAddress::new("/dns4/bootstrap.example.com/tcp/4001/p2p/12D3KooWHybridPeerIdentityExample")
            .expect("valid peer address");
        let err = BootstrapConfig::new(vec![peer.clone(), peer], Some("rustfs".to_string()), 5, 8)
            .expect_err("duplicate bootstrap peers should fail");

        assert_eq!(err, P2pPrimitiveError::DuplicateBootstrapPeer);
    }

    #[test]
    fn p2p_primitives_reject_empty_bootstrap_configuration() {
        let err = BootstrapConfig::new(Vec::new(), None, 5, 8).expect_err("an empty bootstrap config should fail");
        assert_eq!(err, P2pPrimitiveError::MissingBootstrapSource);
    }

    #[test]
    fn p2p_primitives_reject_zero_retry_or_peer_budget() {
        let peer = PeerAddress::new("/dns4/bootstrap.example.com/tcp/4001/p2p/12D3KooWHybridPeerIdentityExample")
            .expect("valid peer address");

        let retry_err = BootstrapConfig::new(vec![peer.clone()], Some("rustfs".to_string()), 0, 8)
            .expect_err("zero retry interval should fail");
        assert_eq!(retry_err, P2pPrimitiveError::InvalidRetryInterval);

        let budget_err =
            BootstrapConfig::new(vec![peer], Some("rustfs".to_string()), 5, 0).expect_err("zero peer budget should fail");
        assert_eq!(budget_err, P2pPrimitiveError::InvalidPeerBudget);
    }

    #[test]
    fn p2p_primitives_reject_inconsistent_config_state() {
        let err = validate_p2p_config(P2pConfig {
            enabled: true,
            local_identity: None,
            bootstrap: None,
        })
        .expect_err("invalid state");
        assert_eq!(err, P2pPrimitiveError::InvalidConfigState);
    }

    #[test]
    fn p2p_config_from_deployment_and_config_builds_enabled_config() {
        let deployment_id = Uuid::new_v4();
        let config = P2pConfig::from_deployment_and_config(
            deployment_id,
            "node-a",
            "12D3KooWHybridPeerIdentityExample",
            vec!["/dns4/bootstrap.example.com/tcp/4001/p2p/12D3KooWHybridPeerIdentityExample".to_string()],
            Some("rustfs".to_string()),
            5,
            8,
        )
        .expect("valid p2p config");

        assert!(config.is_enabled());
        let identity = config.local_identity().expect("identity");
        assert_eq!(identity.deployment_id(), deployment_id);
        assert_eq!(identity.node_name(), "node-a");
        assert_eq!(identity.peer_id().as_str(), "12D3KooWHybridPeerIdentityExample");
        let bootstrap = config.bootstrap().expect("bootstrap");
        assert_eq!(bootstrap.static_peers().len(), 1);
        assert_eq!(bootstrap.rendezvous_namespace(), Some("rustfs"));
        assert_eq!(bootstrap.retry_interval_secs(), 5);
        assert_eq!(bootstrap.max_bootstrap_peers(), 8);
    }

    #[test]
    fn p2p_config_from_deployment_and_config_rejects_nil_deployment_id() {
        let err = P2pConfig::from_deployment_and_config(
            Uuid::nil(),
            "node-a",
            "12D3KooWHybridPeerIdentityExample",
            vec![],
            Some("rustfs".to_string()),
            5,
            8,
        )
        .expect_err("nil deployment id should fail");
        assert_eq!(err, P2pPrimitiveError::NilDeploymentId);
    }
}
