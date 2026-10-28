# How NodusDB works

This page starts from the problem and builds up to the engine one idea at a time. Each section answers a question the one before it leaves open. If you want the exact byte layouts, [architecture.md](architecture.md) has them. If you want the reasons behind each decision, they are in [decisions.md](decisions.md).

## The problem

Picture a graph that changes constantly. An agent writes a fact and forgets another. A user joins a group and another leaves. A session opens, links to a dozen objects and closes. Edges arrive and disappear thousands of times a second, and the code that asked a question wants the answer before the next event shows up.

Two common designs struggle here.

| Design | What it is good at | What goes wrong under churn |
|---|---|---|
| Compressed sparse row (CSR), as in Kùzu and similar engines | Scanning every edge in order | Inserting one edge shifts an array, so a single insert can cost as much as the whole graph |
| A disk B-tree with a recursive query (SQLite and a CTE, for example) | Durability and a familiar language | Every hop of a multi-hop query walks pages, and every write splits or rewrites them |

NodusDB takes a third route. It keeps the whole graph in memory as one small set per node, and it makes every operation on a set cost the same no matter how big the graph is. The rest of this page is how.

## A set as a packed array and an index

Store a set of integers as two arrays.

```
dense     [ 42 | 17 | 99 |  3 | .. | .. ]     members, back to back, positions 0 to degree-1
position  value -> where it sits in dense      one lookup per member
```

Looking up a value reads its position and checks that `dense[position]` is that value. That is two reads. Adding a value appends it to `dense` and records its position. Removing a value copies the last member into the gap and fixes that member's position, so `dense` never has a hole. Briggs and Torczon described this structure in 1993.

Two properties matter. Every operation takes constant time, and a scan reads one array in order, which is the access pattern a CPU handles best. Walking a node's neighbors is a linear sweep of `dense`, with no pointers to chase.

The weak point is the `position` table. Indexed directly by value, it needs one slot for every value that could ever appear, and a node whose neighbors are spread across a huge id range would pay for all of them.

## A hash table instead of a big table

So `position` is an open-addressing hash table. A value hashes to a starting slot, and a lookup walks forward until it finds the value or an empty slot. Three choices keep it fast:

- The hash is the 64-bit finalizer from MurmurHash3, so consecutive ids spread across the table and sequential loads do not form long runs.
- The table doubles once it is half full. Runs stay short, and a lookup usually reads one or two slots.
- Deletion uses backward shift (Knuth, *The Art of Computer Programming*, volume 3, Algorithm R) in place of tombstones. A tombstone marks a slot as "deleted, keep probing", and under churn they pile up until every lookup walks past them. Backward shift moves the entries behind a removed one back into the gap, whenever their probe path crosses it, so the table never fills with dead slots.

There is one trap in backward shift. The table wraps, so a run of entries can cross the last slot and continue at slot 0. A test that compares plain slot numbers moves entries too early or too late at that boundary and loses keys with no error. The shift test must use cyclic distance, `((hole - home) & mask) <= ((slot - home) & mask)`. The test suite pins this case by hand and also by a differential test that compares the table with a `HashSet` after every operation, using keys chosen to collide.

## Small nodes and large nodes

A real graph has a few hubs with thousands of neighbors and a huge number of nodes with a handful. A hash table per node would waste memory on the small ones.

- A node with fewer than 16 neighbors keeps them in one fixed block of 16 `long`s (128 bytes) taken from a shared slab. No per-node object exists. A removal swaps the last neighbor into the gap, and a lookup scans the block, which fits in two cache lines.
- A node with 16 or more neighbors gets its own packed array and hash table from a pool of power-of-two blocks.

A node switches up on its 16th neighbor and back down only when its degree falls to 8. Between 9 and 15 it keeps whatever tier it already has. Without that gap, a node whose degree hovers around 16 would allocate and free a block on every other write.

A node-table slot holds a node's degree and the location of its storage in one 64-bit word, so reaching a node's neighbors takes one read.

## Two directions

Each graph keeps an outgoing table and an incoming table, and every edit updates both. In-degree and "who points at me" cost the same as out-degree and "who do I point at". That matters for permissions, which are mostly asked backwards: not "what can this user see" but "who can see this document".

## Memory the garbage collector never sees

Everything above lives in native memory, not on the Java heap. Storage is a list of chunks, each twice the size of the one before it. Growth adds a chunk and never moves existing data, so a reader holding an address stays valid while a writer grows the table.

This gives three things. The garbage collector has nothing to scan, so a graph of 30 million edges leaves about 1 MB of live Java heap. Every block starts on a 64-byte boundary. And a memory budget is simple to enforce: every chunk is charged against `max_memory_mb` before it is allocated, and a request that does not fit is refused before anything changes.

## Many readers, one writer

The kernel allows one writer and any number of readers, with no lock on the read side. It uses a sequence counter, called a seqlock. The counter is odd while a write is in progress and even otherwise. A reader records the counter, runs its query, and reads the counter again. If it changed, or was odd, a write overlapped the query, so the reader throws the result away and runs the query again.

The design keeps readers cheap and never lets a half-applied change escape. Reads in native code are written so that a torn read can only produce a wrong answer, never an exception, because the retry discards that answer. A stress test runs one writer toggling 200,000 edges on a high-degree node while eight readers run k-hop and common-neighbor queries. Every result must hold distinct, in-range targets. Weakening the validation makes the test fail with a duplicated target.

## Ids, names and the C boundary

The kernel only knows `long` node ids from 0 to a bound that fits a Java array. Names such as `"user:alice"` or a UUID are mapped to dense ids by a symbol table at the edge of the system, and the table is persisted. A graph is keyed by integers or by strings, never both, so the kernel never branches on key type.

The Java code compiles to a shared library with GraalVM Native Image, and Python calls it through `ctypes`. No JVM runs in your process. Every exported function takes an isolate thread, which GraalVM requires, and returns a sentinel instead of throwing, so an exception cannot escape into the host. Failures come back as negative codes from a fixed contract, and the Python layer raises the matching exception. Results are written into caller-supplied buffers, and a call returns the full count so a caller that guessed too small can grow and retry. Batch calls validate the whole batch before they apply any of it.

## Surviving a crash

An in-memory graph is gone when the process ends. A durable graph keeps a directory with two files that matter:

- A **write-ahead log**: typed, checksummed records appended before the change reaches memory. In `sync` mode the call returns only after the record is on disk, so a reader never sees a write that a crash could undo.
- A **snapshot**: a full image of the graph at some log position. A checkpoint writes one to a temporary file, forces it, renames it into place and deletes the log segments it covers.

Recovery loads the snapshot and replays the log after it. Damage in the part of the log known to have reached the disk means corruption, and the directory refuses to open. Damage past that point is a torn tail from a crash, and the log is cut back to its last whole transaction. The crash tests replay every prefix of a run's writes and forces through a simulated disk, with torn writes (every byte for writes up to 64 bytes in the synchronous run, every 13th byte otherwise), and require recovery to return a prefix of what was acknowledged.

Every write returns a token, an epoch and a log position. A reader can pass it as `at_least` to a check, to say "do not answer from state older than this write". This is what keeps a read-after-write honest across a follower or a restart.

## Permissions as edges

A permission check asks whether a subject has a relation to an object, such as `user:alice` viewing `document:readme`. NodusDB stores those facts as typed edges (called tuples) and packs the relation and subject relation into the same 64-bit key as the node id:

```
 63  62           47 46            31 30                         0
+---+---------------+----------------+----------------------------+
| 0 | relation u16  | subj_rel u16   | id (31 bits)               |
+---+---------------+----------------+----------------------------+
```

A key with relation 0 and subject relation 0 equals its id, so an untyped graph needs no extra work. Tuples whose subject is a plain node go in one table pair, so a direct check is one probe. Tuples that name a *set* of subjects (`group:staff#member`) and the links used for inheritance (a document's parent folder) go in a second pair.

A check is a search over (node, definition) pairs. A stored relation probes for the tuple. A permission follows its terms: a computed term asks about the same node under another definition, and an arrow term like `parent->view` asks about each node reached through the parent relation. The search keeps its frames in a per-thread array and its visited set in a generation-stamped table, so it allocates nothing after warm-up and ends on cycles.

Nested groups are the expensive case. A check through a chain of groups would walk every group beneath the one it asks about. The kernel keeps a third table pair holding the transitive closure of that nesting, so a check against the root of a tree of 1,111 groups probes the user's direct memberships against the closure instead of walking the tree. Only changes to nesting touch the closure, and an update that would cost more than 4,096 steps marks the affected groups dirty and leaves them to a background thread. A dirty group is answered by the ordinary evaluator, so every answer stays exact. The closure is derived data and is never written to disk.

## Columns for the lake

The lake buffer applies the same idea in a different shape. Instead of one set per node, it keeps one buffer per **column**. Row *n* is the *n*th entry of every column, and a small hash index maps a key to its row.

```
key_hash   [ k0 | k1 | k2 | ... ]            64-bit key identity
kind       [ INSERT | INSERT | TOMBSTONE ]    one per row
amount     [ 5000 | 1200 | .. ]               int64 or float64 bits
status     [ 1 | 0 | .. ]                     int32
label      offsets + lengths into one shared byte slab
```

An upsert of a known key overwrites its row in place. A delete swaps the last row into the gap across every column, the same trick the graph uses, then fixes the moved key's index entry.

A delete that has to survive a flush cannot just remove the row, because an older committed file may hold a version of that key and removing the row would bring it back. It writes a **tombstone** instead, and the flush writes tombstones to a separate delete file.

Columns are what make the flush cheap. Parquet stores data column by column, so the writer encodes each column from its buffer and streams the pages to a file, with no per-row work. Low-cardinality columns are dictionary-encoded when that is smaller, every chunk records min, max and null counts, and pages are Snappy-compressed.

Two buffers make the writer's life easy. When the active buffer fills, it is frozen and a fresh one takes writes, while a background thread writes the frozen one to Parquet and commits it to a manifest with one atomic rename. Then the frozen buffer is cleared and reused, so its columns keep their grown size and the writing thread stops allocating. If the flush falls two buffers behind, writers wait, and the flush rate sets the ingest rate.

## No allocation on the hot path

Queries and in-tier edits allocate nothing once their structures exist. Allocation happens in three places: a node changes tier, a native buffer grows, or a buffer is replaced. A test reads the thread's allocated-bytes counter across a primed window and requires zero. This matters because a pause from the garbage collector is a pause in your query, and for an embedded engine the pause lands in your process.

## Where to go next

- [architecture.md](architecture.md) has the packages and byte layouts: chunks, blocks, the log and snapshot formats, the chain, the lake buffer.
- [decisions.md](decisions.md) records why each choice was made, with the evidence.
- [designs/](designs/) holds the engineering reviews behind each build step.
- [CONTRIBUTING.md](../CONTRIBUTING.md) tells you how to build it and where help is wanted.
