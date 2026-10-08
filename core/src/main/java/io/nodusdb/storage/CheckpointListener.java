package io.nodusdb.storage;

import java.nio.file.Path;

interface CheckpointListener {

    CheckpointListener NONE = (snapshot, lsn) -> {
    };

    void checkpointed(Path snapshot, long lsn);
}
