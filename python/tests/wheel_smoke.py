import importlib.metadata
import pathlib
import shutil
import tempfile
import uuid

import nodusdb

REPOSITORY = pathlib.Path(__file__).resolve().parents[2]
GOLDEN_INTEGER = REPOSITORY / "core" / "src" / "test" / "resources" / "golden" / "v1" / "integer"
SCHEMA = "schema 1\ntype user\ntype document {\n  relation viewer: user\n  permission view = viewer\n}\n"
LICENSE_FILES = {"LICENSE", "NOTICE", "graalvm-native-image.txt"}
WAIT_SECONDS = 60


def check_installation():
    package_directory = pathlib.Path(nodusdb.__file__).resolve().parent
    assert REPOSITORY / "python" not in package_directory.parents, f"imported from the checkout: {package_directory}"
    distribution = importlib.metadata.distribution("nodusdb")
    assert nodusdb.__version__ == distribution.version, (nodusdb.__version__, distribution.version)
    packaged = {path.name for path in distribution.files or [] if "licenses" in path.parts}
    assert LICENSE_FILES <= packaged, f"license files missing from the wheel: {LICENSE_FILES - packaged}"


def check_graph_basics():
    with nodusdb.Graph() as integers:
        integers.add_edge(1, 2)
        assert integers.has_edge(1, 2)

    with nodusdb.Graph() as strings:
        strings.add_edge("user:alice", "role:admin")
        strings.add_edge(uuid.UUID(int=7), "role:admin")
        assert strings.khop("user:alice", 1) == ["role:admin"]
        assert strings.in_degree("role:admin") == 2


def check_durable_reopen():
    with tempfile.TemporaryDirectory() as directory:
        with nodusdb.Graph(path=directory) as durable:
            durable.add_edge("user:alice", "role:admin")
        with nodusdb.Graph(path=directory) as reopened:
            assert reopened.has_edge("user:alice", "role:admin")


def check_old_directory_upgrade():
    with tempfile.TemporaryDirectory() as directory:
        old = pathlib.Path(directory) / "old"
        shutil.copytree(GOLDEN_INTEGER, old)
        report = nodusdb.upgrade(old)
        assert report.performed and report.edges == 1040, report
        with nodusdb.Graph(path=old) as upgraded:
            assert upgraded.has_edge(1, 2)
        nodusdb.upgrade_cleanup(old)


def check_shipped_features():
    with tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        nodusdb.generate_signing_key(root / "signing.pem", root / "signing.pub")
        shipping = nodusdb.Shipping.directory(
            root / "bucket", key_file=root / "signing.pem", key_id=1, public_key_file=root / "signing.pub",
            interval_ms=10)
        follower = nodusdb.Follower.directory(
            root / "bucket", public_key_file=root / "signing.pub", key_id=1, poll_interval_ms=10)
        with nodusdb.Graph(path=root / "graph", sync_mode="sync", shipping=shipping) as writer:
            writer.apply_schema(SCHEMA)
            before = writer.token
            granted = writer.write(nodusdb.Transaction().add("document:a", "viewer", "user:alice"),
                                   durability="lake")
            assert writer.stats()["shipping"]["shipped_lsn"] >= granted.lsn
            assert any(token_file.suffix == ".obj" for token_file in (root / "bucket" / "_nodus" / "chain").iterdir())

            batch = writer.changes(before)
            assert [change.kind for change in batch.changes].count("add") == 1, batch
            assert batch.next.lsn >= granted.lsn

            writer.checkpoint()
            revoked = writer.write(nodusdb.Transaction().remove("document:a", "viewer", "user:alice"),
                                   durability="lake")
            with writer.as_of(granted) as then, writer.as_of(revoked) as now:
                assert then.check("document:a", "view", "user:alice")
                assert not now.check("document:a", "view", "user:alice")

            writer.write(nodusdb.Transaction().add("document:b", "viewer", "user:bob"), durability="lake")
            with nodusdb.Graph(follow=follower) as reader:
                reader.wait_until_current()
                assert reader.check("document:b", "view", "user:bob")
            restored = nodusdb.restore(follower, root / "restored")
            assert restored.applied_lsn >= granted.lsn
            with nodusdb.Graph(path=root / "restored", sync_mode="sync") as copy:
                assert copy.check("document:b", "view", "user:bob")

            erased = writer.erase("user:bob")
            writer.wait_erased(erased, timeout=WAIT_SECONDS)
            assert not writer.check("document:b", "view", "user:bob")


def check_lake_table():
    with nodusdb.LakeTable(tempfile.mkdtemp(), {"amount": "int64"}) as table:
        table.upsert(1, {"amount": 5})
        assert table.get(1) == {"amount": 5}


def main():
    check_installation()
    check_graph_basics()
    check_durable_reopen()
    check_old_directory_upgrade()
    check_shipped_features()
    check_lake_table()
    print("nodusdb", nodusdb.__version__, nodusdb.__file__, "ok")


if __name__ == "__main__":
    main()
