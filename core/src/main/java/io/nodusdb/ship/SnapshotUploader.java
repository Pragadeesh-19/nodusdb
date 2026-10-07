package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.TransientStoreException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

public final class SnapshotUploader {

    public static final String SHA256_METADATA = "sha256";

    private static final int DEFAULT_CHUNK_BYTES = 8 << 20;

    private record Local(ChainHash hash, long size) {
    }

    private final ObjectStore store;
    private final int chunkBytes;

    public SnapshotUploader(ObjectStore store) {
        this(store, DEFAULT_CHUNK_BYTES);
    }

    SnapshotUploader(ObjectStore store, int chunkBytes) {
        if (chunkBytes < 1) {
            throw new IllegalArgumentException("the verification chunk must hold at least one byte");
        }
        this.store = store;
        this.chunkBytes = chunkBytes;
    }

    public ChainBody.SnapshotRef upload(Path file, long lsn, long chainSeqFloor) {
        Local local = measure(file);
        String key = ChainLayout.snapshotKey(lsn);
        Optional<ObjectInfo> existing = store.head(key);
        if (existing.isPresent()) {
            return adopt(key, lsn, local);
        }
        Map<String, String> metadata = Map.of(
                ChainHead.FLOOR_METADATA, Long.toString(Math.max(1, chainSeqFloor)),
                SHA256_METADATA, local.hash().hex());
        store.putFile(key, file, metadata);
        if (!matches(key, local)) {
            store.delete(key);
            throw new TransientStoreException("the snapshot object for LSN " + lsn
                    + " did not read back identically and was removed", 0);
        }
        return new ChainBody.SnapshotRef(key, local.hash(), lsn);
    }

    private ChainBody.SnapshotRef adopt(String key, long lsn, Local local) {
        if (!matches(key, local)) {
            throw new WriterFencedException("the snapshot object for LSN " + lsn
                    + " already exists with different content; another writer produced it");
        }
        return new ChainBody.SnapshotRef(key, local.hash(), lsn);
    }

    private boolean matches(String key, Local local) {
        Optional<ObjectInfo> info = store.head(key);
        if (info.isEmpty() || info.get().size() != local.size()) {
            return false;
        }
        ChainHash.Accumulator remote = ChainHash.accumulator();
        long offset = 0;
        while (offset < local.size()) {
            int length = (int) Math.min(chunkBytes, local.size() - offset);
            Optional<byte[]> chunk = store.getRange(key, offset, length);
            if (chunk.isEmpty() || chunk.get().length != length) {
                return false;
            }
            remote.update(chunk.get(), 0, length);
            offset += length;
        }
        return remote.finish().equals(local.hash());
    }

    private static Local measure(Path file) {
        ChainHash.Accumulator hash = ChainHash.accumulator();
        byte[] buffer = new byte[1 << 20];
        long size = 0;
        try (InputStream in = Files.newInputStream(file)) {
            int read;
            while ((read = in.read(buffer)) > 0) {
                hash.update(buffer, 0, read);
                size += read;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new Local(hash.finish(), size);
    }
}
