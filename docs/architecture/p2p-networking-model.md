# RustFS P2P Networking Model

**Use this when:** you are changing the hybrid p2p bootstrap path, peer identity or address validation, or the startup/runtime boundary that keeps the new decentralized networking layer additive to the existing HTTP/gRPC control plane.

RustFS currently treats libp2p as an extension point, not a replacement. The network-facing primitives live in `rustfs/src/p2p/mod.rs::P2pConfig`, `NodeIdentity`, `PeerId`, `PeerAddress`, and `BootstrapConfig`, while the cluster-side seed view comes from `crates/ecstore/src/cluster/control_plane.rs::p2p_bootstrap_snapshot_from_membership` and `p2p_bootstrap_snapshot_from_endpoint_pools`. Startup publishes that bootstrap view through `rustfs/src/startup_services.rs::init_startup_runtime_services`, and readiness logging surfaces the peer count from `rustfs/src/startup_lifecycle.rs`.

The current rollout keeps the existing transport stack intact:

- peer discovery can be seeded from endpoint membership or explicit bootstrap peers;
- local nodes are not advertised as bootstrap peers;
- path endpoints remain control-plane membership data, while URL endpoints are the eligible bootstrap addresses;
- the HTTP/gRPC internode paths remain the fallback until the p2p layer is proven stable enough to narrow or replace them.

Keep this document aligned with the current code paths rather than future aspirations. If the bootstrap model changes, update the corresponding code and this document together so the architecture index continues to describe the actual boundary in use.
