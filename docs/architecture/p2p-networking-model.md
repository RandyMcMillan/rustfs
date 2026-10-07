# RustFS P2P Networking Model

**Use this when:** you are changing the hybrid p2p bootstrap path, peer identity or address validation, or the startup/runtime boundary that keeps the new decentralized networking layer additive to the existing HTTP/gRPC control plane.

RustFS currently treats libp2p as an extension point, not a replacement. The network-facing primitives live in `rustfs/src/p2p/mod.rs::P2pConfig`, `NodeIdentity`, `PeerId`, `PeerAddress`, and `BootstrapConfig`, while the cluster-side seed view comes from `crates/ecstore/src/cluster/control_plane.rs::p2p_bootstrap_snapshot_from_membership` and `p2p_bootstrap_snapshot_from_endpoint_pools`. Startup publishes that bootstrap view through `rustfs/src/startup_services.rs::init_startup_runtime_services`, and readiness logging surfaces the peer count from `rustfs/src/startup_lifecycle.rs`.

When the `p2p` feature is enabled and P2P is turned on, `rustfs/src/startup_services.rs::build_p2p_config` resolves a libp2p ed25519 keypair from `--p2p-key-file` / `RUSTFS_P2P_KEY_FILE` or generates an ephemeral keypair and logs a warning. The resolved keypair determines the local peer id; if `--p2p-peer-id` / `RUSTFS_P2P_PEER_ID` is supplied, it must match the keypair. `rustfs/src/p2p/runtime.rs::P2pRuntime::start` then builds a libp2p swarm with TCP, Noise, Yamux, and the identify protocol, listens on `/ip4/0.0.0.0/tcp/0`, dials the configured static bootstrap peers, and runs an event loop that logs identify and connection events. The runtime shuts down when the parent `CancellationToken` fires.

The current rollout keeps the existing transport stack intact:

- peer discovery can be seeded from endpoint membership, explicit bootstrap peers, or the default public IPFS/libp2p bootstrap peers;
- if P2P is enabled and neither static peers nor a rendezvous namespace are supplied, the default IPFS bootstrap list in `rustfs/src/p2p/mod.rs::DEFAULT_P2P_STATIC_PEERS` is used;
- local nodes are not advertised as bootstrap peers;
- path endpoints remain control-plane membership data, while URL endpoints are the eligible bootstrap addresses;
- the HTTP/gRPC internode paths remain the fallback until the p2p layer is proven stable enough to narrow or replace them.

Keep this document aligned with the current code paths rather than future aspirations. If the bootstrap model changes, update the corresponding code and this document together so the architecture index continues to describe the actual boundary in use.
