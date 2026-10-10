import os


def without_none(values):
    return {key: value for key, value in values.items() if value is not None}


def directory_store(path):
    return {"type": "directory", "directory": os.fspath(path)}


def s3_store(bucket, region, prefix=None, endpoint=None, path_style=None, ca_bundle=None):
    return without_none({
        "type": "s3",
        "bucket": bucket,
        "region": region,
        "prefix": prefix,
        "endpoint": endpoint,
        "path_style": path_style,
        "ca_bundle": None if ca_bundle is None else os.fspath(ca_bundle),
    })
