package io.nodusdb.storage;

enum InstallStep {
    STAGING_CREATED,
    SNAPSHOT_WRITTEN,
    TRIPWIRE_WRITTEN,
    FORMAT_WRITTEN,
    TARGET_CLEARED,
    RENAMED
}
