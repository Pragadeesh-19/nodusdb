import tempfile
from pathlib import Path

import nodusdb

SCHEMA = """
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
"""

directory = Path(tempfile.mkdtemp(prefix="nodus-example-")) / "authz"

with nodusdb.Graph(path=directory, sync_mode="sync") as g:
    g.apply_schema(SCHEMA)
    g.add_tuple("document:readme", "parent", "folder:eng")
    g.add_tuple("folder:eng", "viewer", "group:staff", "member")
    g.add_tuple("group:staff", "member", "group:backend", "member")
    token = g.add_tuple("group:backend", "member", "user:alice")

    print("alice can view:", g.check("document:readme", "view", "user:alice", at_least=token))
    print("bob can view:", g.check("document:readme", "view", "user:bob"))

    transaction = nodusdb.Transaction()
    transaction.add("document:plan", "viewer", "user:bob")
    transaction.add("document:plan", "viewer", "user:carol")
    g.write(transaction)
    print("bob can view the plan:", g.check("document:plan", "view", "user:bob"))

    g.remove_tuple("group:backend", "member", "user:alice")
    print("alice after removal:", g.check("document:readme", "view", "user:alice"))
    print("token:", token)
