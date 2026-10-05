package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacySnapshotTest {

    private static final int NODES = 6;

    @TempDir
    Path directory;

    @Test
    void versionOneSnapshotLoadsAndIsRewrittenAsVersionTwo() throws IOException {
        long[][] forward = {{1, 2, 3}, {2}, {}, {0, 4}, {5}, {}};
        Files.write(directory.resolve(DurableStore.SNAPSHOT), legacyFile(forward));

        GraphKernel kernel = GraphKernel.open(directory, WalConfig.DEFAULT);
        assertEdges(kernel, forward);
        kernel.checkpoint();
        kernel.close();

        GraphKernel reopened = GraphKernel.open(directory, WalConfig.DEFAULT);
        try {
            assertEdges(reopened, forward);
            assertEquals(2, readVersion(directory.resolve(DurableStore.SNAPSHOT)));
        } finally {
            reopened.close();
        }
    }

    private static void assertEdges(GraphKernel kernel, long[][] forward) {
        int[] inDegree = new int[NODES];
        for (int u = 0; u < NODES; u++) {
            assertEquals(forward[u].length, kernel.getDegree(u), "out-degree of " + u);
            for (long v : forward[u]) {
                assertTrue(kernel.hasEdge(u, v), "missing edge " + u + "->" + v);
                inDegree[(int) v]++;
            }
        }
        for (int v = 0; v < NODES; v++) {
            assertEquals(inDegree[v], kernel.getInDegree(v), "in-degree of " + v);
        }
    }

    private static byte[] legacyFile(long[][] forward) {
        int edges = 0;
        for (long[] row : forward) {
            edges += row.length;
        }
        ByteBuffer body = ByteBuffer.allocate(28 + NODES * 4 + edges * 8).order(ByteOrder.BIG_ENDIAN);
        body.putInt(SnapshotFile.MAGIC).putShort(SnapshotFile.LEGACY_VERSION).putShort((short) 0)
                .putInt(NODES).putLong(edges).putLong(System.currentTimeMillis());
        for (long[] row : forward) {
            body.putInt(row.length);
            for (long v : row) {
                body.putLong(v);
            }
        }
        byte[] bytes = new byte[body.position()];
        body.flip();
        body.get(bytes);
        CRC32 crc = new CRC32();
        crc.update(bytes);
        ByteBuffer file = ByteBuffer.allocate(bytes.length + 4).order(ByteOrder.BIG_ENDIAN);
        file.put(bytes).putInt((int) crc.getValue());
        return file.array();
    }

    private static int readVersion(Path snapshot) throws IOException {
        ByteBuffer header = ByteBuffer.wrap(Files.readAllBytes(snapshot)).order(ByteOrder.BIG_ENDIAN);
        header.position(4);
        return header.getShort();
    }
}
