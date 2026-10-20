import tempfile
import time
from pathlib import Path

from nodusdb import AccessWatch, Follower, Graph, Shipping, Transaction, generate_signing_key, restore

SCHEMA = """
schema 1
type user
type group { relation member: user | group#member }
type document {
  relation viewer: user | group#member
  permission view = viewer
}
"""

root = Path(tempfile.mkdtemp(prefix="nodus-example-"))
generate_signing_key(root / "signing.pem", root / "signing.pub")

shipping = Shipping.directory(root / "bucket", key_file=root / "signing.pem", key_id=1,
                              public_key_file=root / "signing.pub", interval_ms=20)
follower = Follower.directory(root / "bucket", public_key_file=root / "signing.pub", key_id=1,
                              poll_interval_ms=20)

with Graph(path=root / "graph", sync_mode="sync", shipping=shipping) as writer:
    writer.apply_schema(SCHEMA)
    first = writer.write(Transaction().add("document:plan", "viewer", "user:alice"), durability="lake")
    print("shipped, lag in records:", writer.stats()["shipping"]["lag_lsn"])

    with Graph(follow=follower) as replica:
        replica.wait_until_current()
        print("replica sees alice:", replica.check("document:plan", "view", "user:alice", at_least=first))
        print("replica phase:", replica.stats()["follower"]["phase"])

        watch = AccessWatch(replica, [("document:plan", "view", "user:bob")])
        time.sleep(0.05)
        granted = writer.write(Transaction().add("document:plan", "viewer", "user:bob"), durability="lake")
        for change in watch.poll(timeout=5.0):
            print("access changed:", change.subject, "allowed =", change.allowed)

        cursor = first
        batch = replica.changes(cursor, timeout=5.0)
        for change in batch.changes:
            print("change:", change.kind, change.object, change.relation, change.subject)

    with writer.as_of(first) as then:
        print("as of the first write, bob can view:", then.check("document:plan", "view", "user:bob"))
    with writer.as_of(granted) as then:
        print("as of the second write, bob can view:", then.check("document:plan", "view", "user:bob"))

    erased = writer.erase("user:alice")
    writer.wait_erased(erased, timeout=60)
    print("alice after erasure:", writer.check("document:plan", "view", "user:alice"))

restored = restore(follower, root / "restored")
with Graph(path=root / "restored") as copy:
    print("restored copy, bob can view:", copy.check("document:plan", "view", "user:bob"))
