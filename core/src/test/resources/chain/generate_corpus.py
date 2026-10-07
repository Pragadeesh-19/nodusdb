import hashlib
import json
import struct
import sys
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

DOMAIN = b"nodus.chain.v1\x00"
MAGIC = b"NCHN"
VERSION = 1
KINDS = {"RECORDS": 1, "SNAPSHOT_REF": 2}
RECORDS_FILE = Path(__file__).resolve().parents[1] / "golden" / "v2" / "records.bin"
COMMIT_TYPE = 0x40
AUTOCOMMIT_FLAG = 0x01


def transactions(raw):
    spans = []
    offset = 0
    start = 0
    while offset < len(raw):
        length = struct.unpack_from(">I", raw, offset)[0]
        record_type = raw[offset + 4]
        flags = raw[offset + 5]
        offset += length
        if record_type == COMMIT_TYPE or flags & AUTOCOMMIT_FLAG:
            spans.append(raw[start:offset])
            start = offset
    return spans


def lsn_range(span):
    first = struct.unpack_from(">Q", span, 8)[0]
    offset = 0
    last = first
    while offset < len(span):
        last = struct.unpack_from(">Q", span, offset + 8)[0]
        offset += struct.unpack_from(">I", span, offset)[0]
    return first, last


def seed(label):
    return hashlib.sha256(label.encode()).digest()


def build(entry):
    key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(entry["seed_hex"]))
    body = bytes.fromhex(entry["body_hex"])
    header = struct.pack(">4sHBBQQQ32sII", MAGIC, VERSION, KINDS[entry["kind"]], 0, entry["seq"], entry["epoch"],
                         entry["nonce"], bytes.fromhex(entry["prev_hex"]), entry["key_id"], 0)
    digest = hashlib.sha256(header + body).digest()
    signature = key.sign(DOMAIN + digest)
    public = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    entry["public_hex"] = public.hex()
    entry["digest_hex"] = digest.hex()
    entry["signature_hex"] = signature.hex()
    entry["object_hex"] = (header + body + digest + signature).hex()
    return entry


def records_entry(name, span, seq, epoch, nonce, prev, key_id, label):
    first, last = lsn_range(span)
    body = struct.pack(">QQ", first, last) + span
    return {"name": name, "kind": "RECORDS", "seq": seq, "epoch": epoch, "nonce": nonce, "prev_hex": prev.hex(),
            "key_id": key_id, "seed_hex": seed(label).hex(), "body_hex": body.hex(),
            "fields": {"lsn_first": first, "lsn_last": last, "records_hex": span.hex()}}


def snapshot_entry(name, path, payload, lsn, seq, epoch, nonce, prev, key_id, label):
    encoded = path.encode()
    sha = hashlib.sha256(payload).digest()
    body = struct.pack(">H", len(encoded)) + encoded + sha + struct.pack(">Q", lsn)
    return {"name": name, "kind": "SNAPSHOT_REF", "seq": seq, "epoch": epoch, "nonce": nonce,
            "prev_hex": prev.hex(), "key_id": key_id, "seed_hex": seed(label).hex(), "body_hex": body.hex(),
            "fields": {"path": path, "sha256_hex": sha.hex(), "lsn": lsn}}


def main(target):
    spans = transactions(RECORDS_FILE.read_bytes())
    zero = bytes(32)
    previous = hashlib.sha256(b"previous object").digest()
    entries = [
        records_entry("one autocommit tuple, first object of a chain", spans[2], 1, 1, 0x1122334455667788, zero, 0, "key-a"),
        records_entry("a five record transaction", spans[1], 41, 3, 0xDEADBEEFCAFEF00D, previous, 7, "key-b"),
        records_entry("every record type in one transaction", spans[0], 1 << 40, (1 << 40) + 5, 0, previous, 2147483647, "key-c"),
        records_entry("an autocommit remove at the largest ids", spans[3], 9, 2, 0xFFFFFFFFFFFFFFFF, previous, 1, "key-a"),
        snapshot_entry("snapshot reference", "_nodus/snapshots/00000000000000000042.nsnap", b"snapshot bytes", 42, 1, 1,
                       5, zero, 0, "key-a"),
        snapshot_entry("snapshot reference at lsn zero", "_nodus/snapshots/00000000000000000000.nsnap", b"", 0, 2, 1,
                       6, previous, 3, "key-b"),
        snapshot_entry("snapshot reference with the longest plausible path", "p/" + "d/" * 100 + "x.nsnap",
                       b"x" * 1000, (1 << 62), 123456789, 77, 99, previous, 12, "key-c"),
    ]
    document = {"cryptography": __import__("cryptography").__version__, "entries": [build(entry) for entry in entries]}
    Path(target).write_text(json.dumps(document, indent=1, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {len(entries)} chain objects")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).with_name("corpus.json"))
