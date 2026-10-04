import tempfile
import uuid

import nodusdb


def main():
    with nodusdb.Graph() as integers:
        integers.add_edge(1, 2)
        assert integers.has_edge(1, 2)

    with nodusdb.Graph() as strings:
        strings.add_edge("user:alice", "role:admin")
        strings.add_edge(uuid.UUID(int=7), "role:admin")
        assert strings.khop("user:alice", 1) == ["role:admin"]
        assert strings.in_degree("role:admin") == 2

    with tempfile.TemporaryDirectory() as directory:
        with nodusdb.Graph(path=directory) as durable:
            durable.add_edge("user:alice", "role:admin")
        with nodusdb.Graph(path=directory) as reopened:
            assert reopened.has_edge("user:alice", "role:admin")

    with nodusdb.LakeTable(tempfile.mkdtemp(), {"amount": "int64"}) as table:
        table.upsert(1, {"amount": 5})
        assert table.get(1) == {"amount": 5}

    print("nodusdb", nodusdb.__file__, "ok")


if __name__ == "__main__":
    main()
