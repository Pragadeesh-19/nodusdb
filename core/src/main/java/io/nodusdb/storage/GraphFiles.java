package io.nodusdb.storage;

final class GraphFiles {

    static final String FORMAT = "FORMAT";
    static final String SNAPSHOT = "snapshot.bin";
    static final String SNAPSHOT_TEMP = "snapshot.bin.tmp";
    static final String TRIPWIRE = "nodus.wal";
    static final String TRIPWIRE_TEMP = "nodus.wal.tmp";
    static final String LEGACY_SYMBOLS = "symbols.nodus";
    static final String LOG_DIRECTORY = "log";
    static final String SHIP_STAGING = "ship-staging";
    static final String SHIP_SCRATCH = "ship-scratch";
    static final String LOCK = "nodus.lock";
    static final String BACKUP_DIRECTORY = "pre-v2";
    static final String STAGING_DIRECTORY = ".upgrade";

    private GraphFiles() {
    }
}
