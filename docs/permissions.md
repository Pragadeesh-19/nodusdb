# Permissions

A guide to using the graph as a relationship-based access system, in the style of Google's Zanzibar. You describe your types in a small schema, store who is related to what as tuples, and ask `check("document:readme", "view", "user:alice")`. The answer is a `bool`. The runnable version of this page is [`examples/03_permissions.py`](../examples/03_permissions.py).

## The idea

Access rules in most applications are the same shape: *Alice can view this document because she belongs to a group that can view the folder that contains it.* Writing that as a SQL join or a recursive query works until the groups nest five deep and every request runs it.

A relationship-based system stores the facts and the rules separately:

- A **tuple** is one fact: `document:readme` has the relation `parent` to `folder:eng`. Or `group:staff` has the relation `member` to `user:carol`.
- A **schema** is the rules: a document's `view` permission is whoever is a `viewer` of it, plus whoever can `view` its parent.
- A **check** applies the rules to the facts and answers yes or no.

Facts change all the time, so they are stored as edges in the same engine that handles graph churn. A grant or a revoke is one edge write.

## Names

Every object and subject is written `type:id`, for example `user:alice`, `group:staff`, `document:readme`. The type must be one the schema declares. The id can be anything. Prefer opaque ids such as UUIDs for people; the [erasure](operations.md#erasure) section explains why.

## The schema

```
schema 1
type user
type group { relation member: user | group#member }
type folder {
  relation viewer: user | group#member
  relation parent: folder
  permission view = viewer + parent->view
}
type document {
  relation parent: folder
  relation viewer: user | group#member
  permission view = viewer + parent->view
}
```

Read it top to bottom:

- `schema 1` is the version. A new schema must be exactly one higher than the current one.
- `type user` declares a type with nothing in it. Users are subjects only.
- `relation member: user | group#member` declares a stored relation and the kinds of subject it accepts. Here a group's members are users, or the members of another group (`group#member` means "the set of subjects that have `member` on a group"). That second form is how groups nest.
- `permission view = viewer + parent->view` declares a computed answer built from terms joined by `+`. A bare name such as `viewer` means "anyone with that relation or permission on this object". `parent->view` means "follow the `parent` relation, then ask `view` on whatever it reaches". The `->` is how a document inherits from its folder.

The language has unions only. It has no intersection or exclusion.

A schema moves forward one version at a time, and relation ids are never reused, so an old log or snapshot always decodes. A relation can be removed in a later version, and its id stays retired. A version that turns a relation holding tuples into a link relation (one used with `->`), or retires it, is refused. Moving the tuples between the two tables in the same transaction is not built yet.

## Writing facts

```python
import nodusdb

with nodusdb.Graph(path="/data/authz", sync_mode="sync") as g:
    g.apply_schema(SCHEMA)

    g.add_tuple("document:readme", "parent", "folder:eng")
    g.add_tuple("folder:eng", "viewer", "group:staff", "member")
    g.add_tuple("group:staff", "member", "group:backend", "member")
    token = g.add_tuple("group:backend", "member", "user:alice")
```

`add_tuple(object, relation, subject, subject_relation=None)` stores one fact. The fourth argument is for subjects that are sets: `("folder:eng", "viewer", "group:staff", "member")` reads "every member of `group:staff` is a viewer of `folder:eng`". `remove_tuple` takes the same arguments. Adding a fact that exists, or removing one that does not, writes nothing.

A tuple is checked against the schema when you write it. An unknown type, a relation the type does not have, or a subject the relation does not accept raises `NodusSchemaError` and changes nothing.

To apply several facts all or nothing, use a transaction:

```python
transaction = nodusdb.Transaction()
transaction.add("document:plan", "viewer", "user:bob")
transaction.add("document:plan", "viewer", "user:carol")
g.write(transaction)
```

Recovery applies whole transactions only. A transaction must fit in one 1 MiB write, so split a bigger load. For bulk loads of plain edges, use `add_edges_from`.

## Asking

```python
g.check("document:readme", "view", "user:alice", at_least=token)    # True
g.check("document:readme", "view", "user:bob")                       # False
```

`check(object, permission, subject)` returns `True` or `False`. It also works with a stored relation as the second argument, so `check("group:staff", "member", "user:carol")` asks about direct membership.

A check that cannot decide within the depth limit of 32 raises `NodusCheckDepthError`. It does not answer `False`, because a wrong "no" is the worse failure. A grant found on any branch is still returned even when another branch ran out of depth.

Checks take no lock. Many threads can check against one graph while another thread writes.

## Tokens: reading your own writes

Every write returns a `Token(epoch, lsn)`, a name for that exact point in the log. Pass it as `at_least` to make a check refuse to answer from older state:

```python
token = g.add_tuple("group:backend", "member", "user:alice")
g.check("document:readme", "view", "user:alice", at_least=token)
```

A token the graph itself returned is always satisfied there. It matters on a [follower](operations.md#followers-restore-and-takeover), which may lag the writer: the check waits (up to `read_wait_ms`) for the follower to reach the token, then raises `NodusStaleReadError`. If the write was acknowledged but a crash or takeover dropped it, the check raises `NodusTokenLostError`. That error exists so a grant you were told succeeded never silently disappears.

`durability="lake"` on `write` goes one step further and waits until the write is in the object store. See [Operations](operations.md#shipping-the-log).

## Nested groups and the reachability index

The expensive case in any permission system is nesting. Alice is in `backend`, which is in `eng`, which is in `staff`. A check against `staff` has to find her through the chain, and a big organization has trees of hundreds or thousands of groups.

NodusDB keeps a derived table of the transitive closure of group nesting, so a check against the root of a deep tree probes the user's direct memberships against that table in one step. It is updated inside the same write as the tuple that changes the nesting. An update that would cost more than 4,096 steps marks the affected groups dirty instead and leaves them to a background thread, and a dirty group is answered by the ordinary search, so every answer stays exact. Tuples that do not change nesting (adding a user to a group, for example) cost no index work.

Measured on the development laptop (`bench/baseline/reachability-windows-dev.json`):

| Check | With the index | Without |
|---|---:|---:|
| Against the root of a tree of 1,111 nested groups | about 0.7 µs | 110 to 116 µs |
| Through a chain of 30 nested groups | 0.65 µs | 2.5 to 3.5 µs |
| Adding or removing a direct member | same either way | same either way |
| Nesting and unnesting a leaf group (two writes) | 7.7 µs | 2.2 µs |

The index is never written to disk. A graph rebuilds it after a restart, and checks use the ordinary search, with its depth limit, until it is ready. `stats()["reachability"]` shows its state. Set `NODUS_REACHABILITY_INDEX=off` to turn it off.

On the permission workload in the benchmark suite (10,000 users, 2,000 groups, 50,000 resources, 81,500 membership edges), the median check took 38 µs and a membership update 9.5 µs. NetworkX took 171 µs for the check and SQLite 400 µs. That run used the library as it was on 2026-10-04.

## Watching who gains and loses access

`AccessWatch` re-checks a list of triples whenever the graph commits, and tells you which answers flipped:

```python
from nodusdb import AccessWatch

watch = AccessWatch(g, [("document:plan", "view", "user:alice")])
for change in watch.poll(timeout=1.0):
    if not change.allowed:
        disconnect(change.subject, change.object)
```

This is how a long-lived connection learns that its owner lost access. [Operations](operations.md#watching-changes) describes the change feed underneath it.

## Erasing a person

`g.erase("user:alice")` removes every tuple that names her and replaces her name with a pseudonym in memory, in the graph directory, in the bucket and in the Iceberg table. [Operations](operations.md#erasure) explains the layers and what erasure cannot reach.
