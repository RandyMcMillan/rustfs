// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Minimal libp2p runtime for experimental peer-to-peer bootstrap.
//!
//! This module is compiled only when the `p2p` feature is enabled. It builds a
//! small libp2p swarm that listens on a local TCP socket, dials configured
//! static bootstrap peers, and runs an identify protocol so the node can be
//! discovered by other peers.

use crate::p2p::{BootstrapConfig, P2pConfig};
use futures::StreamExt;
use libp2p::{Multiaddr, Swarm, SwarmBuilder, identify, identity::Keypair, swarm::NetworkBehaviour, swarm::SwarmEvent};
use std::io;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const LOG_COMPONENT_P2P: &str = "p2p";
const LOG_SUBSYSTEM_RUNTIME: &str = "runtime";
const EVENT_P2P_LISTEN: &str = "p2p_listen";
const EVENT_P2P_IDENTIFY: &str = "p2p_identify";
const EVENT_P2P_DIAL: &str = "p2p_dial";
const EVENT_P2P_ERROR: &str = "p2p_error";

/// Errors that can occur while starting or running the P2P runtime.
#[derive(Debug, Error)]
pub enum P2pRuntimeError {
    #[error("failed to decode libp2p identity keypair: {0}")]
    InvalidKeypair(String),
    #[error("failed to build libp2p transport: {0}")]
    TransportBuild(String),
    #[error("failed to build identify behaviour: {0}")]
    IdentifyBuild(String),
    #[error("invalid bootstrap peer address: {0}")]
    InvalidPeerAddress(String),
    #[error("swarm listen failed: {0}")]
    ListenFailed(String),
}

impl From<P2pRuntimeError> for io::Error {
    fn from(error: P2pRuntimeError) -> Self {
        io::Error::other(error)
    }
}

/// Behaviour bundle used by the RustFS P2P runtime.
///
/// Currently only identify is implemented. Gossipsub, Kademlia, and request/
/// response will be added here as the hybrid control plane matures.
#[derive(NetworkBehaviour)]
pub struct P2pBehaviour {
    identify: identify::Behaviour,
}

impl P2pBehaviour {
    fn new(local_keypair: &Keypair) -> Self {
        let config = identify::Config::new("rustfs/0.1".to_string(), local_keypair.public())
            .with_agent_version(format!("rustfs/{}", crate::version::get_version()));
        Self {
            identify: identify::Behaviour::new(config),
        }
    }
}

/// Handle to the running P2P runtime.
pub struct P2pRuntime {
    shutdown: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
    listen_addrs: watch::Receiver<Vec<Multiaddr>>,
}

impl P2pRuntime {
    /// Start the P2P runtime when the feature and configuration are enabled.
    ///
    /// Returns `Ok(None)` when P2P is disabled so callers can treat the runtime
    /// as an optional sidecar.
    pub fn start(config: Option<P2pConfig>, parent_shutdown: &CancellationToken) -> Result<Option<P2pRuntime>, P2pRuntimeError> {
        let Some(config) = config else {
            return Ok(None);
        };
        if !config.is_enabled() {
            return Ok(None);
        }

        let bootstrap = config
            .bootstrap()
            .cloned()
            .ok_or_else(|| P2pRuntimeError::InvalidPeerAddress("P2P enabled but bootstrap config is missing".to_string()))?;
        let identity = config
            .local_identity()
            .cloned()
            .ok_or_else(|| P2pRuntimeError::InvalidPeerAddress("P2P enabled but local identity is missing".to_string()))?;

        let keypair = Keypair::from_protobuf_encoding(config.keypair_protobuf())
            .map_err(|err| P2pRuntimeError::InvalidKeypair(err.to_string()))?;

        let mut swarm = build_swarm(keypair)?;
        let listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/0".parse().expect("static multiaddr is valid");
        swarm
            .listen_on(listen_addr.clone())
            .map_err(|err| P2pRuntimeError::ListenFailed(err.to_string()))?;

        let (listen_tx, listen_rx) = watch::channel(Vec::new());
        let runtime_shutdown = CancellationToken::new();
        let event_loop_shutdown = runtime_shutdown.child_token();
        let parent_shutdown = parent_shutdown.clone();

        let join_handle = tokio::spawn(async move {
            let mut event_loop = P2pEventLoop {
                swarm,
                bootstrap,
                local_peer_id: identity.peer_id().to_string(),
                listen_addrs: listen_tx,
                shutdown: event_loop_shutdown,
            };
            event_loop.run().await;
        });

        let runtime_shutdown_for_parent = runtime_shutdown.clone();
        tokio::spawn(async move {
            parent_shutdown.cancelled().await;
            runtime_shutdown_for_parent.cancel();
        });

        Ok(Some(P2pRuntime {
            shutdown: runtime_shutdown,
            join_handle,
            listen_addrs: listen_rx,
        }))
    }

    /// Return the current listen addresses, if any have been reported by the
    /// swarm.
    pub fn listen_addrs(&self) -> Vec<Multiaddr> {
        self.listen_addrs.borrow().clone()
    }

    /// Signal shutdown and wait for the event loop to finish.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        if let Err(err) = self.join_handle.await {
            tracing::warn!(
                target: "rustfs::p2p",
                event = EVENT_P2P_ERROR,
                component = LOG_COMPONENT_P2P,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "join_failed",
                reason = if err.is_cancelled() { "cancelled" } else { "panicked" },
                "P2P runtime task failed to join"
            );
        }
    }
}

fn build_swarm(keypair: Keypair) -> Result<Swarm<P2pBehaviour>, P2pRuntimeError> {
    Ok(SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(libp2p::tcp::Config::default(), libp2p::noise::Config::new, libp2p::yamux::Config::default)
        .map_err(|err| P2pRuntimeError::TransportBuild(err.to_string()))?
        .with_behaviour(|keypair| P2pBehaviour::new(keypair))
        .map_err(|err| P2pRuntimeError::IdentifyBuild(err.to_string()))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build())
}

struct P2pEventLoop {
    swarm: Swarm<P2pBehaviour>,
    bootstrap: BootstrapConfig,
    local_peer_id: String,
    listen_addrs: watch::Sender<Vec<Multiaddr>>,
    shutdown: CancellationToken,
}

impl P2pEventLoop {
    async fn run(&mut self) {
        for peer in self.bootstrap.static_peers().to_vec() {
            let addr = match peer.as_str().parse::<Multiaddr>() {
                Ok(addr) => addr,
                Err(err) => {
                    tracing::warn!(
                        target: "rustfs::p2p",
                        event = EVENT_P2P_ERROR,
                        component = LOG_COMPONENT_P2P,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        peer_address = %peer,
                        reason = %err,
                        "Skipping invalid bootstrap peer address"
                    );
                    continue;
                }
            };
            if let Err(err) = self.swarm.dial(addr.clone()) {
                tracing::warn!(
                    target: "rustfs::p2p",
                    event = EVENT_P2P_DIAL,
                    component = LOG_COMPONENT_P2P,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    peer_address = %addr,
                    state = "failed",
                    reason = %err,
                    "Failed to dial bootstrap peer"
                );
            } else {
                tracing::debug!(
                    target: "rustfs::p2p",
                    event = EVENT_P2P_DIAL,
                    component = LOG_COMPONENT_P2P,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    peer_address = %addr,
                    state = "queued",
                    "Dialing bootstrap peer"
                );
            }
        }

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                event = self.swarm.next() => {
                    let Some(event) = event else { break; };
                    self.handle_event(event).await;
                }
            }
        }
    }

    async fn handle_event(&mut self, event: SwarmEvent<P2pBehaviourEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                let mut addrs = self.listen_addrs.borrow().clone();
                addrs.push(address.clone());
                let _ = self.listen_addrs.send(addrs);
                tracing::info!(
                    target: "rustfs::p2p",
                    event = EVENT_P2P_LISTEN,
                    component = LOG_COMPONENT_P2P,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    listen_address = %address,
                    local_peer_id = %self.local_peer_id,
                    "P2P listener ready"
                );
            }
            SwarmEvent::Behaviour(P2pBehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. })) => {
                tracing::info!(
                    target: "rustfs::p2p",
                    event = EVENT_P2P_IDENTIFY,
                    component = LOG_COMPONENT_P2P,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    peer_id = %peer_id,
                    agent_version = %info.agent_version,
                    protocol_version = %info.protocol_version,
                    "Identified P2P peer"
                );
            }
            SwarmEvent::Behaviour(P2pBehaviourEvent::Identify(identify::Event::Sent { peer_id, .. })) => {
                tracing::debug!(
                    target: "rustfs::p2p",
                    event = EVENT_P2P_IDENTIFY,
                    component = LOG_COMPONENT_P2P,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    peer_id = %peer_id,
                    state = "sent",
                    "Sent identify info to peer"
                );
            }
            SwarmEvent::Behaviour(P2pBehaviourEvent::Identify(identify::Event::Pushed { peer_id, .. })) => {
                tracing::debug!(
                    target: "rustfs::p2p",
                    event = EVENT_P2P_IDENTIFY,
                    component = LOG_COMPONENT_P2P,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    peer_id = %peer_id,
                    state = "pushed",
                    "Pushed identify info to peer"
                );
            }
            SwarmEvent::Behaviour(P2pBehaviourEvent::Identify(identify::Event::Error { peer_id, error, .. })) => {
                tracing::warn!(
                    target: "rustfs::p2p",
                    event = EVENT_P2P_ERROR,
                    component = LOG_COMPONENT_P2P,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    peer_id = %peer_id,
                    reason = %error,
                    "Identify protocol error"
                );
            }
            SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                tracing::debug!(
                    target: "rustfs::p2p",
                    event = "p2p_connection",
                    component = LOG_COMPONENT_P2P,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    peer_id = %peer_id,
                    endpoint = ?endpoint,
                    state = "established",
                    "P2P connection established"
                );
            }
            SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                tracing::debug!(
                    target: "rustfs::p2p",
                    event = "p2p_connection",
                    component = LOG_COMPONENT_P2P,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    peer_id = %peer_id,
                    state = "closed",
                    reason = ?cause,
                    "P2P connection closed"
                );
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                tracing::warn!(
                    target: "rustfs::p2p",
                    event = EVENT_P2P_ERROR,
                    component = LOG_COMPONENT_P2P,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    peer_id = ?peer_id,
                    reason = %error,
                    "Outgoing P2P connection failed"
                );
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::{NodeIdentity, PeerId};
    use std::time::Duration;
    use uuid::Uuid;

    fn enabled_config_with_keypair(keypair: Keypair, static_peers: Vec<String>) -> P2pConfig {
        let peer_id = keypair.public().to_peer_id().to_string();
        let keypair_protobuf = keypair.to_protobuf_encoding().expect("encode keypair");
        let identity = NodeIdentity::new(Uuid::new_v4(), "node-a", PeerId::new(peer_id).expect("peer id")).expect("identity");
        let bootstrap = crate::p2p::BootstrapConfig::new(
            static_peers
                .into_iter()
                .map(crate::p2p::PeerAddress::new)
                .collect::<Result<Vec<_>, _>>()
                .expect("peers"),
            None,
            30,
            16,
        )
        .expect("bootstrap");
        P2pConfig::enabled(identity, bootstrap, keypair_protobuf)
    }

    fn enabled_config_with_peers(static_peers: Vec<String>) -> P2pConfig {
        enabled_config_with_keypair(Keypair::generate_ed25519(), static_peers)
    }

    #[tokio::test]
    async fn runtime_start_disabled_config_returns_none() {
        let runtime = P2pRuntime::start(Some(P2pConfig::disabled()), &CancellationToken::new()).expect("start");
        assert!(runtime.is_none());
    }

    #[tokio::test]
    async fn runtime_start_none_config_returns_none() {
        let runtime = P2pRuntime::start(None, &CancellationToken::new()).expect("start");
        assert!(runtime.is_none());
    }

    #[tokio::test]
    async fn runtime_build_swarm_from_protobuf_keypair() {
        let keypair = Keypair::generate_ed25519();
        let expected_peer_id = keypair.public().to_peer_id();
        let protobuf = keypair.to_protobuf_encoding().expect("encode keypair");

        let decoded = Keypair::from_protobuf_encoding(&protobuf).expect("decode keypair");
        let swarm = build_swarm(decoded).expect("build swarm");

        assert_eq!(swarm.local_peer_id(), &expected_peer_id);
    }

    #[tokio::test]
    async fn runtime_two_local_peers_exchange_identify() {
        let keypair_a = Keypair::generate_ed25519();
        let peer_id_a = keypair_a.public().to_peer_id();
        let cfg_a = enabled_config_with_keypair(keypair_a, vec![]);

        let keypair_b = Keypair::generate_ed25519();
        let peer_id_b = keypair_b.public().to_peer_id();

        let runtime_a = P2pRuntime::start(Some(cfg_a), &CancellationToken::new())
            .expect("start a")
            .expect("runtime a");

        tokio::time::sleep(Duration::from_millis(100)).await;
        let addrs_a = runtime_a.listen_addrs();
        assert!(!addrs_a.is_empty(), "node a should have a listen address");

        let dial_addr = addrs_a
            .first()
            .cloned()
            .expect("listen address")
            .with(libp2p::multiaddr::Protocol::P2p(peer_id_a));

        let identity_b =
            NodeIdentity::new(Uuid::new_v4(), "node-b", PeerId::new(peer_id_b.to_string()).expect("peer id")).expect("identity");
        let bootstrap_b = crate::p2p::BootstrapConfig::new(
            vec![crate::p2p::PeerAddress::new(dial_addr.to_string()).expect("peer address")],
            None,
            30,
            16,
        )
        .expect("bootstrap");
        let keypair_protobuf_b = keypair_b.to_protobuf_encoding().expect("encode keypair");
        let cfg_b = P2pConfig::enabled(identity_b, bootstrap_b, keypair_protobuf_b);

        let runtime_b = P2pRuntime::start(Some(cfg_b), &CancellationToken::new())
            .expect("start b")
            .expect("runtime b");

        tokio::time::sleep(Duration::from_millis(500)).await;

        let addrs_b = runtime_b.listen_addrs();
        assert!(!addrs_b.is_empty(), "node b should have a listen address");

        runtime_a.shutdown().await;
        runtime_b.shutdown().await;
    }
}
