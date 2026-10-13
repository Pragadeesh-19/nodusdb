# Engineering review: Build Step 4, the reachability index

Status: decisions D139 to D152 accepted on 2026-10-13. Implementation scope is Build Step 4 (section 11), built on
branch feat/authz-step4. Decisions D153 to D158 record what the build refined (`docs/decisions.md`).
Contracts: [`engineering-review-authz-wedge.md`](engineering-review-authz-wedge.md) sections 3, 5, 10 and 12. Where
this document and those disagree about Build Step 4, this document wins (section 2 lists every amendment).

Step 4 makes a check through nested groups cost a set intersection instead of a walk. The kernel keeps the transitive
closure of group nesting next to the tuples, updates it inside the same write section as the tuple that changed it,
and gives up on a group (marks it dirty) when an update would cost more than a fixed budget. Dirty groups are answered
by the existing evaluator, so every answer stays exact. A background maintainer rebuilds dirty groups in small slices.

---

## 1. First principles

1. **The index is derived.** It is a function of the stored tuples and the schema. It is never written to the log or
   the snapshot, it can be thrown away at any moment, and a graph without it answers the same.
2. **An answer is exact or it is not given.** The index answers only for groups whose closure is known to be complete.
   Everything else goes to the evaluator (D76).
3. **A write never fails because of the index.** Index work happens after the record is journaled. A missing budget,
   a missing memory reservation or a cap marks groups dirty; it never raises.
4. **Bounded work per write.** One write does at most `W_max` units of index work inside its seqlock section.
5. **The common write is free.** Adding or removing a member of a group (a direct tuple) changes no closure.

```
 tuple write ──> RecordApplier.applyTuple ──> EdgeTables (DIRECT / INDIRECT)
                                 │
                       nesting tuple?  (relation has MEMBERSHIP, subject relation != 0)
                                 │ yes
                                 v
                        ClosureUpdater ── within W_max and memory ──> CLOSURE tables
                                 │ over budget
                                 v
                        ClosureIndex.markDirty(node) ──> ClosureMaintainer (thread, slices)
                                                                │
 check ──> CheckEvaluator.stepStored ──> clean group? ── yes ──> ClosureIndex.isMember (intersection)
                                                │ no
                                                └──────────────> evaluator descent (D76), unchanged
```

## 2. Amendments to the frozen design

| Where | Text before | Text now | Decision |
|---|---|---|---|
| Wedge 10 | `MEMBER2GROUP(u)` is a structure | it is read from the incoming adjacency of `u`, filtered to membership relations with subject relation 0 | D141 |
| Wedge 10 | rebuilds run on the writer between writes | a maintainer thread runs the slices, started on demand, and yields to queued writers | D146, D147 |
| Wedge 12 | a check deeper than 32 raises `NodusCheckDepthError` | a clean group answers without descending, so its depth does not count; dirty groups keep the limit | D142 |
| Wedge 10 | dirty per group | dirty per object node (all membership relations of the node) | D144 |

## 3. Decision log

| ID | Issue | Decision |
|---|---|---|
| D139 | Scope | The frozen design: incremental closure updates on nesting add and remove within `W_max`, dirty fallback, sliced rebuild, stats, oracle tests |
| D140 | Closure storage | A third `EdgeTables` pair, `CLOSURE`, owned by `GraphState` and created on first use. An entry is a derived tuple `g#r@d#r2` meaning every member of `d#r2` is a member of `g#r`, packed with `EdgeKey` exactly like a stored tuple. Outgoing is keyed by the ancestor node, incoming by the descendant node. Native memory, the graph's `MemoryBudget`, and the same seqlock read safety |
| D141 | MEMBER2GROUP | Not materialized. The incoming tables of `u` already hold `pack(r, 0, g)` for every direct membership. A direct membership write costs no index work |
| D142 | Depth limit | A clean group answers through the index, so nesting depth below it does not count toward the limit of 32. Dirty groups use the evaluator and keep the limit |
| D143 | Indexed nodes | A userset `(g, r)` is indexed when `r` has the `MEMBERSHIP` flag. A nesting edge is a stored tuple `g#r@c#r2` with `r` membership and `r2 != 0`. Documents and other objects whose relations are not used as `T#rel` get no closure, which keeps memory proportional to the group graph |
| D144 | Dirty granularity | One dirty bit per object node, plus a full-pass sweep line: while a full pass runs, every node at or above the line is dirty |
| D145 | Schema change | A `SCHEMA` record that changes any `MEMBERSHIP` flag starts a full pass. Snapshot load and bulk load start a full pass |
| D146 | Who rebuilds | A `ClosureMaintainer` thread per kernel, started the first time a check meets a dirty node (or a write dirties one after checks began). It exits when there is no work and starts again on demand. `close()` stops it. Throwaway kernels (salvage, upgrade, restore staging) never start it |
| D147 | Writer priority | The maintainer takes the writer lock with `tryLock` and backs off while writers are queued, so a user write waits for at most one slice |
| D148 | Memory cap | Closure entries are capped at `max(1,048,576, 4 x stored tuples)` and by the graph's memory budget. An update that would pass either marks its groups dirty |
| D149 | Code shape | Package `io.nodusdb.kernel.closure`: `ClosureLimits`, `ClosureIndex` (reads and dirty state), `ClosureUpdater` (incremental work), `ClosureRebuild` (resumable per-node job), `ClosureMaintainer` (thread). `GraphKernel`, `GraphState` and `RecordApplier` gain delegating calls only |
| D150 | Kill switch | `ClosureLimits` is a kernel option. `NODUS_REACHABILITY_INDEX=off`, read once per kernel, disables the index. Stats say whether it is on. No ABI change |
| D151 | Work unit | One unit is one adjacency key visited, one closure key read, written or removed, or one dirty mark. `W_max` is 4096 units |
| D152 | Writes and slices | Writes do not run rebuild slices. A write does only its own bounded incremental update; the maintainer does the rest |

## 4. Definitions and invariants

- **Userset** `(g, r)`: an object node `g` and a membership relation `r`.
- **Nesting edge** `(g, r) -> (c, r2)`: a stored tuple `g#r@c#r2`.
- **closure(g, r)**: every userset reachable from `(g, r)` through one or more nesting edges. With a cycle it contains
  `(g, r)` itself.
- **members(g, r)** contains `u` when `g#r@u` is stored, or some `(d, r3)` in closure(g, r) has `d#r3@u` stored.
- **R1**: for every node `g` that is not dirty, the outgoing `CLOSURE` keys of `g` are exactly
  `{ pack(r, r2, d) : (d, r2) in closure(g, r) }` over all membership relations `r`, and every outgoing key has its
  mirror in the incoming table. Checked by an oracle after every operation in the tests.
- **R2**: a node's dirty state changes only inside a seqlock write section. A reader that sees a node clean and
  validates its read section saw a closure that satisfied R1.

The evaluator follows the same edges: a stored frame `(g, r)` probes `g#r@u` directly, then descends into every
INDIRECT key of `g` with relation `r` and a non-zero subject relation. The closure is the transitive version of that
step, so an index answer equals an evaluator answer by construction.

## 5. Check with the index

```
member(g, r, u):                                  inside the evaluator's read section
  probe g#r@u                                     direct, already done by stepStored
  if CLOSURE.degree(g) <= inDegree(u):            sweep the smaller side
      for key in CLOSURE.outgoing(g) with relation r:
          (d, r3) = (id, subject relation) of key
          if probe d#r3@u: return true
  else:
      for key in incoming(u) (DIRECT, then INDIRECT) with subject relation 0 and a membership relation:
          (h, r4) = (id, relation) of key
          if CLOSURE.outgoing(g) contains pack(r, r4, h): return true
  return false
```

Cost is `O(min(|closure of g|, in-degree of u))` probes. `CLOSURE.degree(g)` counts every membership relation of `g`,
and `inDegree(u)` counts every relation, so both are upper bounds; the side choice is a heuristic and never affects the
answer.

## 6. Incremental updates

Both run inside the write section of the tuple, after the tuple is applied, and only when the tuple actually changed.

```
insert nesting edge (p, r) -> (c, r2):
  A = {(p, r)} + clean ancestors of (p, r)        incoming CLOSURE keys of p with subject relation r
  if |A| > W_max: start a full pass; return
  if c is dirty: mark every node of A dirty; return
  D = {(c, r2)} + closure(c, r2)                  read into writer scratch before writing anything
  if |A| * |D| > W_max, or the cap or the memory headroom refuses: mark every node of A dirty; return
  reserve headroom; add every missing (a -> d) to CLOSURE

remove nesting edge (p, r) -> (c, r2):
  A = {(p, r)} + clean ancestors
  if |A| > W_max: start a full pass; return
  D = {(c, r2)} + closure(c, r2)                  only these can leave a closure
  if c is clean and no member of A holds itself (no cycle through A):
      repeat until a pass removes nothing:
          for each a in A and each d in D that a holds:
              keep d if a child x of a is d or holds d; otherwise remove it
      a dirty child or an exhausted budget: mark every node of A dirty
  otherwise: recompute each node of A by breadth-first search over the tuples, within W_max in total,
             and mark every node of A dirty when that runs out
```

Removal never adds keys, so it needs no memory. Without a cycle through `A` the nesting among the ancestors is
acyclic, so the repeated pass has one fixed point, the true closure, and reaches it in at most the depth of the
chain. Children outside `A` cannot reach `p`, so their closures did not change and can be trusted (D158). Insertion is correct with cycles: a new path from an ancestor `a`
goes `a ... p -> c` and then follows edges from `c`, all of which closure(c) already holds; reading closure(c) before
writing keeps the result independent of the order.

## 7. Rebuild and the maintainer

`ClosureRebuild` rebuilds one node at a time: collect the closure of every membership relation of the node by
breadth-first search over the INDIRECT tuples, then diff against the node's current `CLOSURE` keys, then clear its
dirty bit. It is resumable: each slice does at most `W_max` units and keeps its frontier in writer scratch. A
`nestingVersion` counter increases on every nesting change and every full-pass start; a job that sees a different
version than it started with restarts. The node stays dirty until the job finishes, so readers never use a partial
closure.

```
 ClosureMaintainer.run:
   while work remains and the kernel is open:
     if writers are queued: back off briefly
     else if writer.tryLock():
         beginWrite; rebuild.runSlice(W_max); endWrite; unlock
   exit (a later demand starts a new thread)
```

A full pass walks node ids upward from the sweep line. Each node is rebuilt, or skipped when it has neither nesting
tuples nor closure keys.

## 8. Failure modes

| Codepath | Failure | Test | Handling | Caller sees |
|---|---|---|---|---|
| insert | closure would exceed the cap | cap test with a small cap | mark the ancestors dirty | exact answer through the evaluator |
| insert | memory budget refuses | small `max_memory` test | mark dirty, nothing allocated | exact answer; the write succeeds |
| insert, remove | thousands of ancestors | `W_max = 4` tests | full pass | exact answer through the evaluator |
| insert | dirty child | unit | mark the ancestors dirty | exact answer |
| remove | recompute over budget | `W_max` tests | mark dirty | exact answer |
| cycle | self-nesting, two-node and longer cycles | oracle seeds | BFS handles cycles; closure contains the node itself | exact answer |
| schema | `MEMBERSHIP` flips either way | unit | full pass | exact answer during the pass |
| open, follower bootstrap | snapshot loaded without the applier | load test | full pass | evaluator until rebuilt |
| maintainer | kernel closed during a slice | close test | stop flag, join | nothing |
| maintainer | rebuild restarted by churn | version test | restart; node stays dirty | exact answer |
| reader | read overlaps a slice | concurrency stress | seqlock retry | exact answer |
| kill switch | env set to off | unit | index never built | evaluator answers |

No row is silent with neither a test nor handling.

## 9. Test plan

```
CODE PATHS                                          TESTS
ClosureIndex      dirty bit, sweep line, member()    ClosureIndexTest, oracle R1, differential
ClosureUpdater    insert: in budget / A>W_max /       ClosureUpdaterTest (W_max 4 and 4096), oracle
                  dirty child / cap / memory
                  remove: recompute / over budget
ClosureRebuild    collect, diff, resume, restart      ClosureRebuildTest, oracle
ClosureMaintainer start on demand, exit, restart,     ClosureMaintainerTest
                  yield to writers, close
RecordApplier     nesting vs direct vs untyped        ClosureOracleTest via the kernel
SchemaGuard path  MEMBERSHIP flips                    ClosureSchemaTest
load paths        snapshot, bulk, follower, replay    ClosureLoadTest, follower test
CheckEvaluator    clean / dirty / disabled, depth     ReachabilityCheckTest, differential vs naive
stats             reachability block                  GraphSessionStatsTest, Python test
concurrency       readers during slices and writes    ClosureConcurrencyTest, contention gate
allocation        clean check allocates nothing       ReachabilityAllocationTest
```

The oracle drives seeded random operations over a small universe (groups, two membership relations, users) with
cycles, removals and schema flips. After every operation it checks R1 for every clean node and compares every check
with a naive recursive evaluator that has no depth limit.

## 10. Configuration and stats

`ClosureLimits(enabled, workBudget = 4096, minimumCap = 1,048,576, capPerTuple = 4)`, with
`NODUS_REACHABILITY_INDEX=off` turning `enabled` off. The stats document gains:

```
"reachability": {"enabled": true, "state": "current" | "building", "dirty_nodes": 3, "closure_entries": 1840,
                 "entry_cap": 1048576, "incremental_updates": 120, "dirty_marks": 4, "rebuilt_nodes": 12,
                 "refusals": 0, "full_passes": 1, "last_error": ""}
```

## 11. Sequencing

| # | Change | Verify |
|---|---|---|
| 4.1 | Tuple counter in `EdgeTables`; `ClosureLimits`; `GraphState` owns the `CLOSURE` pair | existing suite |
| 4.2 | `ClosureIndex`: dirty bits, sweep line, `isMember` | unit tests |
| 4.3 | `ClosureRebuild`: collect, diff, resume, restart | unit tests, R1 |
| 4.4 | `ClosureUpdater` and the `RecordApplier`, schema and load hooks | oracle, R1 after every operation |
| 4.5 | `ClosureMaintainer` and the `GraphKernel` wiring | lifecycle tests, contention gate |
| 4.6 | `CheckEvaluator` uses the index | differential test, allocation test |
| 4.7 | Stats, docs, decisions, benchmark and baseline | Python suite, JMH |

### 11.1 Expectations to measure (not claims)

- A deny check over 1,000 nested groups drops from one probe per reachable group to `min(closure, in-degree)`
  probes.
- A direct membership write costs the same as before (one flag lookup).
- A slice of 4,096 units takes tens of microseconds, so a reader overlapping a slice retries once.

Measured (`bench/baseline/reachability-windows-dev.json`, D158): a check against the root of 1,111 nested groups took 0.7 us against 110 to 116 us without the index, a chain of 30 took 0.65 us against 2.5 to 3.5 us, a direct member add and remove pair cost 2.0 us against 1.9 us, and a nesting add and remove pair at a leaf of that tree 7.7 us against 2.2 us.

## 12. NOT in scope

- Persisting the closure in the snapshot (TODOS, P3; a snapshot format change).
- A public API switch for the index (env and kernel option only, D150).
- Intersection and exclusion in the schema language (TODOS, P2).
- Closures for relations that are not membership relations (D143).

## 13. What already exists

| Need | Existing | Reused |
|---|---|---|
| Closure storage, budget, read safety | `EdgeTables`, `AdjacencyTable`, `MemoryBudget`, seqlock | yes, a third pair |
| All-or-nothing memory | `Headroom`, `AdditionCounter`, `WriteReservation` pattern | yes |
| MEMBER2GROUP | incoming adjacency of the subject | yes, no new structure |
| Membership flag | `RelationCatalog.MEMBERSHIP`, set by `SchemaCompiler` | yes |
| One mutation choke point | `RecordApplier.applyTuple` and `applyAbsentAddition` | yes |
| Exact fallback | `CheckEvaluator` (D76) | yes, unchanged for dirty groups |

## 14. Parallelization

Sequential implementation, no parallelization opportunity: every step touches `io.nodusdb.kernel` and builds on the
previous one.
