import os
import sys
import tempfile
import time
import uuid
from pathlib import Path

ENDPOINT_BUCKET = "nodus-tls-probe-no-such-bucket-"
REACHED_MARKERS = ("InvalidAccessKeyId", "SignatureDoesNotMatch", "HTTP 403")
TLS_MARKERS = ("PKIX", "certification path", "SSLHandshake", "certificate", "handshake", "trustAnchors")
ATTEMPTS = 3
PAUSE_SECONDS = 5

REACHED = "reached"
TLS = "tls"
UNREACHABLE = "unreachable"


def classify(message):
    if any(marker in message for marker in TLS_MARKERS):
        return TLS
    if any(marker in message for marker in REACHED_MARKERS):
        return REACHED
    return UNREACHABLE


def forget_java_home():
    for name in [name for name in os.environ if name == "JAVA_HOME" or name.startswith("JAVA_HOME_")]:
        del os.environ[name]


def attempt():
    import nodusdb

    root = Path(tempfile.mkdtemp(prefix="nodus-https-probe-"))
    nodusdb.generate_signing_key(root / "signing.pem", root / "signing.pub")
    shipping = nodusdb.Shipping.s3(
        ENDPOINT_BUCKET + uuid.uuid4().hex[:8],
        "us-east-1",
        key_file=root / "signing.pem",
        key_id=1,
        public_key_file=root / "signing.pub",
        credentials=nodusdb.static_credentials("AKIAEXAMPLEFAKE00000", "fakeSecretKeyForTlsProbeOnly000000000000"),
    )
    try:
        nodusdb.Graph(path=root / "graph", shipping=shipping).close()
    except nodusdb.NodusError as error:
        return classify(str(error)), str(error)
    return UNREACHABLE, "the graph opened against a bucket that cannot exist"


def main():
    forget_java_home()
    verdict, message = UNREACHABLE, ""
    for number in range(1, ATTEMPTS + 1):
        verdict, message = attempt()
        print(f"attempt {number}: {verdict}: {message[:300]}")
        if verdict != UNREACHABLE:
            break
        time.sleep(PAUSE_SECONDS)
    if verdict == REACHED:
        print("HTTPS to S3 works with the default trust store")
        return 0
    if verdict == TLS:
        print("error: the native library does not trust the S3 certificate; set ca_bundle or fix the trust store",
              file=sys.stderr)
    else:
        print("error: could not reach https://s3.amazonaws.com", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
