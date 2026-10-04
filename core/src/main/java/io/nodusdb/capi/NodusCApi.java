package io.nodusdb.capi;

import io.nodusdb.kernel.wal.SyncMode;
import io.nodusdb.kernel.wal.WalConfig;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.VoidPointer;
import org.graalvm.word.WordFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public final class NodusCApi {

    private static final int ERROR = -1;
    private static final int MAX_PATH_BYTES = 32_768;
    private static final GraphSessions SESSIONS = new GraphSessions();

    private NodusCApi() {
    }

    @CEntryPoint(name = "nodus_create")
    public static VoidPointer create(IsolateThread thread) {
        try {
            return WordFactory.pointer(SESSIONS.open());
        } catch (RuntimeException e) {
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "nodus_destroy")
    public static int destroy(IsolateThread thread, VoidPointer handle) {
        try {
            SESSIONS.close(handle.rawValue());
            return 0;
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_open_durable")
    public static VoidPointer openDurable(IsolateThread thread, CCharPointer directory, int syncMode) {
        try {
            WalConfig config = WalConfig.withSyncMode(SyncMode.fromCode(syncMode));
            return WordFactory.pointer(SESSIONS.openDurable(Path.of(cString(directory)), config));
        } catch (IOException | RuntimeException e) {
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "nodus_checkpoint")
    public static int checkpoint(IsolateThread thread, VoidPointer handle) {
        try {
            session(handle).checkpoint();
            return 0;
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    private static String cString(CCharPointer pointer) {
        int length = 0;
        while (pointer.read(length) != 0) {
            length++;
            if (length > MAX_PATH_BYTES) {
                throw new IllegalArgumentException("path longer than " + MAX_PATH_BYTES + " bytes");
            }
        }
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = pointer.read(i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @CEntryPoint(name = "nodus_add_edge")
    public static boolean addEdge(IsolateThread thread, VoidPointer handle, long u, long v) {
        try {
            return session(handle).addEdge(u, v);
        } catch (RuntimeException e) {
            return false;
        }
    }

    @CEntryPoint(name = "nodus_remove_edge")
    public static boolean removeEdge(IsolateThread thread, VoidPointer handle, long u, long v) {
        try {
            return session(handle).removeEdge(u, v);
        } catch (RuntimeException e) {
            return false;
        }
    }

    @CEntryPoint(name = "nodus_has_edge")
    public static boolean hasEdge(IsolateThread thread, VoidPointer handle, long u, long v) {
        try {
            return session(handle).hasEdge(u, v);
        } catch (RuntimeException e) {
            return false;
        }
    }

    @CEntryPoint(name = "nodus_degree")
    public static int degree(IsolateThread thread, VoidPointer handle, long u) {
        try {
            return session(handle).degree(u);
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_in_degree")
    public static int inDegree(IsolateThread thread, VoidPointer handle, long v) {
        try {
            return session(handle).inDegree(v);
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_add_edges_batch")
    public static int addEdgesBatch(IsolateThread thread, VoidPointer handle, CLongPointer uArr,
                                    CLongPointer vArr, int count) {
        try {
            GraphSession session = session(handle);
            stageEdges(session, uArr, vArr, count);
            return session.addEdges(count);
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_remove_edges_batch")
    public static int removeEdgesBatch(IsolateThread thread, VoidPointer handle, CLongPointer uArr,
                                       CLongPointer vArr, int count) {
        try {
            GraphSession session = session(handle);
            stageEdges(session, uArr, vArr, count);
            return session.removeEdges(count);
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_common_neighbors")
    public static int commonNeighbors(IsolateThread thread, VoidPointer handle, long u, long v, CLongPointer outBuf, int outCap) {
        try {
            GraphSession session = session(handle);
            int total = session.commonNeighbors(u, v);
            copyResults(session, total, outBuf, outCap);
            return total;
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_khop")
    public static int khop(IsolateThread thread, VoidPointer handle, long start, int maxDepth, CLongPointer outBuf, int outCap) {
        try {
            GraphSession session = session(handle);
            int total = session.khop(start, maxDepth);
            copyResults(session, total, outBuf, outCap);
            return total;
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    private static GraphSession session(VoidPointer handle) {
        return SESSIONS.get(handle.rawValue());
    }

    private static void stageEdges(GraphSession session, CLongPointer uArr, CLongPointer vArr, int count) {
        long[] staged = session.edgeBuffer(count);
        for (int i = 0; i < count; i++) {
            staged[2 * i] = uArr.read(i);
            staged[2 * i + 1] = vArr.read(i);
        }
    }

    private static void copyResults(GraphSession session, int total, CLongPointer outBuf, int outCap) {
        int count = Math.min(total, Math.max(outCap, 0));
        for (int i = 0; i < count; i++) {
            outBuf.write(i, session.result(i));
        }
    }
}
