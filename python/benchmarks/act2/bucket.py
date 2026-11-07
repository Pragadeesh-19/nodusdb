"""The S3 bucket Act 2 ships to, asked only what any outside reader can ask of it."""

from dataclasses import dataclass

from botocore.exceptions import ClientError

MISSING = ("NoSuchKey", "404")


class BucketError(RuntimeError):
    pass


@dataclass(frozen=True)
class Leftovers:
    noncurrent_versions: tuple[str, ...]
    delete_markers: tuple[str, ...]

    def any(self):
        return bool(self.noncurrent_versions or self.delete_markers)


class S3Objects:
    def __init__(self, client, bucket, prefix):
        self._client = client
        self._bucket = bucket
        self._root = prefix.strip("/") + "/"

    def keys(self, prefix):
        keys = []
        for page in self._client.get_paginator("list_objects_v2").paginate(Bucket=self._bucket,
                                                                           Prefix=self._root + prefix):
            keys.extend(entry["Key"][len(self._root):] for entry in page.get("Contents", ()))
        return keys

    def read(self, key):
        try:
            return self._client.get_object(Bucket=self._bucket, Key=self._root + key)["Body"].read()
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in MISSING:
                return None
            raise

    def location(self, key):
        return f"s3://{self._bucket}/{self._root}{key}"


def require_never_versioned(client, bucket):
    status = client.get_bucket_versioning(Bucket=bucket).get("Status")
    if status is not None:
        raise BucketError(f"bucket {bucket} reports versioning {status}; a bucket that ever kept versions can hold "
                          f"copies an erasure cannot reach")
    return status


def require_empty(objects):
    found = objects.keys("")
    if found:
        raise BucketError(f"the prefix already holds {len(found)} objects, first {found[0]}; use an empty prefix")


def leftovers(client, bucket, prefix):
    noncurrent, markers = [], []
    for page in client.get_paginator("list_object_versions").paginate(Bucket=bucket, Prefix=prefix.strip("/") + "/"):
        noncurrent.extend(version["Key"] for version in page.get("Versions", ()) if not version.get("IsLatest", True))
        markers.extend(marker["Key"] for marker in page.get("DeleteMarkers", ()))
    return Leftovers(tuple(noncurrent), tuple(markers))


def require_no_leftovers(client, bucket, prefix):
    found = leftovers(client, bucket, prefix)
    if found.any():
        raise BucketError(f"the bucket keeps {len(found.noncurrent_versions)} old versions and "
                          f"{len(found.delete_markers)} delete markers under the prefix")
    return found
