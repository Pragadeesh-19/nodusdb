import json
import sys

try:
    import fastavro
except ImportError:
    sys.exit(77)


def plain(value):
    if isinstance(value, (bytes, bytearray)):
        return {"hex": bytes(value).hex()}
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


def metadata_text(metadata):
    result = {}
    for key, value in metadata.items():
        result[key] = value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else value
    return result


def main(path):
    with open(path, "rb") as handle:
        reader = fastavro.reader(handle)
        records = [plain(record) for record in reader]
        print(json.dumps({
            "schema": reader.writer_schema,
            "metadata": metadata_text(reader.metadata),
            "codec": reader.codec,
            "records": records,
        }))


if __name__ == "__main__":
    main(sys.argv[1])
