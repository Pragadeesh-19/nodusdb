import datetime
import json
import random
import sys
from pathlib import Path

import botocore.auth
from botocore.auth import S3SigV4Auth, SIGV4_TIMESTAMP
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

SEED = 20261007
RANDOM_CASES = 150
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
TOKEN = "IQoJb3JpZ2luX2VjEPr//////////wEaCXVzLWVhc3QtMSJHMEUCIQDtoken+with/special=chars=="
REGIONS = ["us-east-1", "eu-west-2", "ap-southeast-1", "us-west-2", "sa-east-1"]
DATES = ["20260101T000000Z", "20240229T235959Z", "20261231T235959Z", "20251007T120501Z", "20380119T031407Z"]
HOSTS = ["s3.amazonaws.com", "mybucket.s3.us-east-1.amazonaws.com", "127.0.0.1:9000", "localhost:18080",
         "minio.internal.example.com", "s3.eu-west-2.amazonaws.com"]
SEGMENTS = ["_nodus", "chain", "epoch", "snapshots", "iceberg", "data", "metadata", "a-b", "a.b", "x_1", "00000000000000000042.obj",
            "v12.metadata.json", "snap-1-1-uuid.avro", "a%20b", "%E2%82%AC", "caf%C3%A9", "a%2Bb"]
QUERY_NAMES = ["list-type", "prefix", "start-after", "max-keys", "encoding-type", "uploads", "uploadId",
               "partNumber", "delete", "versionId", "continuation-token"]
QUERY_VALUES = ["2", "", "_nodus%2Fchain%2F", "a%20b", "1000", "url", "abc%2Bdef%3D", "caf%C3%A9", "~tilde", "x.y-z_w", "%E2%82%AC"]
HEADER_NAMES = ["if-none-match", "x-amz-meta-note", "x-amz-meta-chain-seq", "range", "content-md5", "content-type",
                "x-amz-meta-k", "If-None-Match", "X-Amz-Meta-Mixed-Case"]
HEADER_VALUES = ["*", "hello", "42", "bytes=0-9", "1B2M2Y8AsgTpgAmY7PhCfg==", "application/octet-stream", "  padded  value  ",
                 "two   spaces", "tab\tseparated", "a,b,c", "\"quoted\""]
METHODS = ["GET", "PUT", "POST", "DELETE", "HEAD"]


def case(name, method, host, path, query, headers, body, region, date, token=None):
    return {"name": name, "method": method, "host": host, "path": path, "query": query, "headers": headers,
            "body_hex": body.hex(), "region": region, "date": date, "access_key": ACCESS_KEY,
            "secret_key": SECRET_KEY, "token": token}


def fixed_cases():
    return [
        case("get object", "GET", "s3.amazonaws.com", "/examplebucket/test.txt", "", {}, b"", "us-east-1", "20130524T000000Z"),
        case("put object with metadata and conditional header", "PUT", "s3.amazonaws.com", "/examplebucket/chain/00000000000000000042.obj", "",
             {"if-none-match": "*", "x-amz-meta-note": "hello"}, b"Welcome to Amazon S3.", "us-east-1", "20130524T000000Z"),
        case("range read", "GET", "s3.amazonaws.com", "/examplebucket/test.txt", "", {"range": "bytes=0-9"}, b"", "us-east-1", "20130524T000000Z"),
        case("list objects", "GET", "s3.amazonaws.com", "/examplebucket/", "list-type=2&prefix=_nodus%2Fchain%2F&start-after=_nodus%2Fchain%2F00000000000000000041.obj&max-keys=1000&encoding-type=url",
             {}, b"", "us-east-1", "20260101T000000Z"),
        case("initiate multipart upload", "POST", "s3.amazonaws.com", "/examplebucket/snapshots/1.nsnap", "uploads", {"x-amz-meta-chain-seq": "7"}, b"", "us-east-1", "20260101T000000Z"),
        case("upload part", "PUT", "s3.amazonaws.com", "/examplebucket/snapshots/1.nsnap", "partNumber=3&uploadId=abc%2Bdef%3D", {}, b"part body", "eu-west-2", "20260101T000000Z"),
        case("delete objects", "POST", "s3.amazonaws.com", "/examplebucket/", "delete", {"content-md5": "1B2M2Y8AsgTpgAmY7PhCfg=="}, b"<Delete></Delete>", "us-east-1", "20260101T000000Z"),
        case("session token", "GET", "s3.amazonaws.com", "/examplebucket/test.txt", "", {}, b"", "us-east-1", "20260101T000000Z", TOKEN),
        case("host with port", "PUT", "127.0.0.1:9000", "/bucket/_nodus/epoch/00000000000000000003.json", "", {"if-none-match": "*"}, b"{}", "us-east-1", "20260101T000000Z"),
        case("virtual hosted style", "GET", "mybucket.s3.us-east-1.amazonaws.com", "/_nodus/chain/1.obj", "", {}, b"", "us-east-1", "20260101T000000Z"),
        case("header whitespace is collapsed", "PUT", "s3.amazonaws.com", "/b/k", "", {"x-amz-meta-note": "  a   b \t c  "}, b"x", "us-east-1", "20260101T000000Z"),
        case("header names are lowercased", "PUT", "s3.amazonaws.com", "/b/k", "", {"If-None-Match": "*", "X-Amz-Meta-Foo": "Bar"}, b"x", "us-east-1", "20260101T000000Z"),
        case("query is sorted and repeated names order by value", "GET", "s3.amazonaws.com", "/b/", "b=&a=2&a=1&c=~", {}, b"", "us-east-1", "20260101T000000Z"),
        case("percent encoded path is signed as sent", "GET", "s3.amazonaws.com", "/b/a%20b/%E2%82%AC.txt", "", {}, b"", "us-east-1", "20260101T000000Z"),
        case("encoded query values", "GET", "s3.amazonaws.com", "/b/", "prefix=a%20b%2Fc~d%C3%A9", {}, b"", "us-east-1", "20260101T000000Z"),
        case("leap day", "HEAD", "s3.amazonaws.com", "/b/k", "", {}, b"", "ap-southeast-1", "20240229T235959Z"),
        case("year boundary", "DELETE", "s3.amazonaws.com", "/b/k", "", {}, b"", "us-west-2", "20261231T235959Z"),
        case("empty key path root", "GET", "s3.amazonaws.com", "/", "", {}, b"", "us-east-1", "20260101T000000Z"),
        case("large binary body", "PUT", "s3.amazonaws.com", "/b/blob", "", {}, bytes(range(256)) * 40, "us-east-1", "20260101T000000Z"),
    ]


def random_cases():
    rng = random.Random(SEED)
    cases = []
    for index in range(RANDOM_CASES):
        host = rng.choice(HOSTS)
        path = "/" + "/".join(rng.choice(SEGMENTS) for _ in range(rng.randint(1, 4)))
        pairs = [(rng.choice(QUERY_NAMES), rng.choice(QUERY_VALUES)) for _ in range(rng.randint(0, 4))]
        query = "&".join(name if value == "" and rng.random() < 0.3 else f"{name}={value}" for name, value in pairs)
        headers = {}
        for _ in range(rng.randint(0, 4)):
            name = rng.choice(HEADER_NAMES)
            if name.lower() not in {existing.lower() for existing in headers}:
                headers[name] = rng.choice(HEADER_VALUES)
        body = bytes(rng.randrange(256) for _ in range(rng.choice([0, 0, 1, 7, 64, 1000])))
        token = TOKEN if rng.random() < 0.25 else None
        cases.append(case(f"random {index}", rng.choice(METHODS), host, path, query, headers, body,
                          rng.choice(REGIONS), rng.choice(DATES), token))
    return cases


def expected(entry):
    moment = datetime.datetime.strptime(entry["date"], "%Y%m%dT%H%M%SZ")
    botocore.auth.get_current_datetime = lambda: moment
    credentials = Credentials(entry["access_key"], entry["secret_key"], entry["token"])
    url = "http://" + entry["host"] + entry["path"] + ("?" + entry["query"] if entry["query"] else "")
    request = AWSRequest(method=entry["method"], url=url, headers=dict(entry["headers"]), data=bytes.fromhex(entry["body_hex"]))
    auth = S3SigV4Auth(credentials, "s3", entry["region"])
    request.context["timestamp"] = moment.strftime(SIGV4_TIMESTAMP)
    auth._modify_request_before_signing(request)
    canonical = auth.canonical_request(request)
    string_to_sign = auth.string_to_sign(request, canonical)
    signature = auth.signature(string_to_sign, request)
    auth._inject_signature_to_request(request, signature)
    headers = {name.lower(): value for name, value in request.headers.items()}
    produced = {name: headers[name] for name in ("x-amz-date", "x-amz-content-sha256", "x-amz-security-token", "authorization") if name in headers}
    return {"payload_sha256": headers["x-amz-content-sha256"], "canonical_request": canonical,
            "string_to_sign": string_to_sign, "signature": signature, "headers": produced}


def main(target):
    vectors = []
    for entry in fixed_cases() + random_cases():
        entry["expected"] = expected(entry)
        vectors.append(entry)
    Path(target).write_text(json.dumps({"botocore": botocore.__version__, "vectors": vectors}, indent=1, sort_keys=True) + "\n",
                            encoding="utf-8", newline="\n")
    print(f"wrote {len(vectors)} vectors with botocore {botocore.__version__}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).with_name("vectors.json"))
