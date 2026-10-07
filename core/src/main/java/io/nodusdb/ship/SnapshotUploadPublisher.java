package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;

import java.io.IOException;
import java.io.UncheckedIOException;

public final class SnapshotUploadPublisher implements SnapshotPublisher {

    private final SnapshotSource source;
    private final SnapshotUploader uploader;

    public SnapshotUploadPublisher(SnapshotSource source, SnapshotUploader uploader) {
        this.source = source;
        this.uploader = uploader;
    }

    @Override
    public ChainBody.SnapshotRef publish(long chainSeqFloor) {
        try (StagedSnapshot staged = source.stage()) {
            return uploader.upload(staged.file(), staged.lsn(), chainSeqFloor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
