# TODOS

## Authorization

### Decision log for permission checks

**What:** Record each check and its answer in a sampled, append-only decisions table written through Spillway's append mode.

**Why:** Auditors and incident responders ask "who was actually allowed in", and the edge log only answers "who could have been".

**Context:** Deferred in the 2026-10-06 CEO review (issue 9B) until a first audit customer sets sampling and retention needs. It reuses the append-only table mode, signed commits and the edge-log shipper from the relationship-lakehouse plan. The check path must stay near 40 us, so writes go through a lock-free ring drained off the hot path.

**Effort:** M
**Priority:** P2
**Depends on:** Spillway append-only mode, edge-log shipper, signed commits

### Iceberg REST catalog commit target

**What:** A second commit implementation that registers and commits the edge log through the Iceberg REST protocol (Polaris, Unity, Glue), beside the S3 compare-and-swap committer.

**Why:** Enterprise data platforms discover and govern tables through their catalog; without this, the edge log has to be registered by hand.

**Context:** Alternative 3B in the 2026-10-06 CEO review. The default stays zero-infrastructure (S3 conditional writes plus a writer epoch). The ObjectStore and commit interfaces from issue 11A make this pluggable.

**Effort:** M
**Priority:** P2
**Depends on:** S3 CAS committer, ObjectStore interface

### Intersection and exclusion in the schema language

**What:** Schema version 2 adds `&` (intersection) and `-` (exclusion) to permissions, with evaluator and Leopard support.

**Why:** Some enterprise models need "must be an employee and on the project" or "everyone except blocked users".

**Context:** D60 reserves the syntax and schema version 1 rejects it, so this needs no log format change. Exclusion can't be precomputed safely, so a check must still distinguish "denied because excluded" from "gave up" (D76). See `docs/designs/engineering-review-authz-wedge.md` section 5.

**Effort:** L
**Priority:** P2
**Depends on:** D76 check evaluator (Build Step 1), Leopard (Build Step 4)

## Engine

### Deterministic simulation harness for the distributed steps

**What:** Run the writer, the lake shipper, a fake S3 and followers on a simulated clock, inject failures from a seed, and replay any failure exactly.

**Why:** Split-brain, lost-write and stale-read bugs live in rare interleavings of the commit protocol, followers and takeover; seeded simulation finds and reproduces them.

**Context:** Gate for Build Step 3. The local every-prefix crash harness (D73) is the seed. The protocol to simulate is in `docs/designs/engineering-review-authz-wedge.md` sections 8 and 9.

**Effort:** L
**Priority:** P1
**Depends on:** Build Step 1 (LogStore I/O seam)

### Low-latency writer-to-follower tail stream

**What:** An optional TCP stream of committed records from the writer to followers, verified against the signed lake chain.

**Why:** S3 replication gives about 200 to 500 ms visibility (D66); some customers will need near-instant follower reads.

**Context:** Tokens and dense LSNs already support it. It adds a network surface on the writer (auth, backpressure), which is why it waits for demand. S3 stays the durable and trusted path.

**Effort:** M
**Priority:** P3
**Depends on:** Build Step 3

### Automatic failover with leases

**What:** A writer lease object in S3, renewed by the writer, with automatic takeover after expiry and epoch fencing.

**Why:** Hands-off recovery for teams without on-call, instead of the manual audited takeover (D65).

**Context:** Needs bounded clock skew, and still loses unshipped `LOCAL` writes unless callers use `LAKE` durability. The epoch and CAS design in section 8 of the design doc is lease-ready.

**Effort:** M
**Priority:** P3
**Depends on:** Build Step 3, the deterministic simulation harness

### More than one writer through namespace sharding

**What:** One writer and one signed edge log per namespace (tenant or resource type), with cross-namespace checks routed or federated.

**Why:** A single writer tops out around 80,000 mixed changes per second; namespace sharding lifts that without consensus and keeps every per-log guarantee.

**Context:** Multi-writer support is out of scope in the current design. Epochs, signatures and LSNs stay per log. The hard part is group nesting across namespaces, since the reachability index is per kernel.

**Effort:** XL
**Priority:** P3
**Depends on:** The full relationship-lakehouse plan shipping first

## Completed
