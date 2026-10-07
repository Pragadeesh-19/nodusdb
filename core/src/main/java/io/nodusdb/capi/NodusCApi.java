package io.nodusdb.capi;

import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.memory.MemoryLimitExceededException;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CIntPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.VoidPointer;
import org.graalvm.word.WordFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public final class NodusCApi {

    private static final int OK = 0;
    private static final int ERROR = -1;
    private static final int LOOKUP_ERROR = -2;
    private static final int MEMORY_LIMIT = -3;
    private static final long MAX_STRING_BYTES = Integer.MAX_VALUE - 8;
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

    @CEntryPoint(name = "nodus_create_limited")
    public static VoidPointer createLimited(IsolateThread thread, long maxMemoryBytes, CIntPointer status) {
        try {
            VoidPointer handle = WordFactory.pointer(SESSIONS.open(maxMemoryBytes));
            report(status, OK);
            return handle;
        } catch (MemoryLimitExceededException e) {
            report(status, MEMORY_LIMIT);
        } catch (RuntimeException e) {
            report(status, ERROR);
        }
        return WordFactory.nullPointer();
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
            LogConfig config = LogConfig.withSyncMode(SyncMode.fromCode(syncMode));
            return WordFactory.pointer(SESSIONS.openDurable(Path.of(cString(directory)), config));
        } catch (IOException | RuntimeException e) {
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "nodus_open_durable_limited")
    public static VoidPointer openDurableLimited(IsolateThread thread, CCharPointer directory, int syncMode,
                                                 long maxMemoryBytes, CIntPointer status) {
        try {
            LogConfig config = LogConfig.withSyncMode(SyncMode.fromCode(syncMode));
            VoidPointer handle = WordFactory.pointer(
                    SESSIONS.openDurable(Path.of(cString(directory)), config, maxMemoryBytes));
            report(status, OK);
            return handle;
        } catch (MemoryLimitExceededException e) {
            report(status, MEMORY_LIMIT);
        } catch (IOException | RuntimeException e) {
            report(status, ERROR);
        }
        return WordFactory.nullPointer();
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

    @CEntryPoint(name = "nodus_sync")
    public static int sync(IsolateThread thread, VoidPointer handle) {
        try {
            session(handle).sync();
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
    public static int addEdge(IsolateThread thread, VoidPointer handle, long u, long v) {
        try {
            return session(handle).addEdge(u, v) ? 1 : 0;
        } catch (MemoryLimitExceededException e) {
            return MEMORY_LIMIT;
        } catch (RuntimeException e) {
            return ERROR;
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
            synchronized (session) {
                stageEdges(session, uArr, vArr, count);
                return session.addEdges(count);
            }
        } catch (MemoryLimitExceededException e) {
            return MEMORY_LIMIT;
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_remove_edges_batch")
    public static int removeEdgesBatch(IsolateThread thread, VoidPointer handle, CLongPointer uArr,
                                       CLongPointer vArr, int count) {
        try {
            GraphSession session = session(handle);
            synchronized (session) {
                stageEdges(session, uArr, vArr, count);
                return session.removeEdges(count);
            }
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_common_neighbors")
    public static int commonNeighbors(IsolateThread thread, VoidPointer handle, long u, long v, CLongPointer outBuf, int outCap) {
        try {
            GraphSession session = session(handle);
            synchronized (session) {
                int total = session.commonNeighbors(u, v);
                copyResults(session, total, outBuf, outCap);
                return total;
            }
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_khop")
    public static int khop(IsolateThread thread, VoidPointer handle, long start, int maxDepth, CLongPointer outBuf, int outCap) {
        try {
            GraphSession session = session(handle);
            synchronized (session) {
                int total = session.khop(start, maxDepth);
                copyResults(session, total, outBuf, outCap);
                return total;
            }
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_key_kind")
    public static int keyKind(IsolateThread thread, VoidPointer handle) {
        try {
            return session(handle).keyKind().code();
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_claim_key_kind")
    public static int claimKeyKind(IsolateThread thread, VoidPointer handle, int kind) {
        try {
            return session(handle).claimKeys(KeyKind.fromCode(kind)).code();
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_intern")
    public static long intern(IsolateThread thread, VoidPointer handle, CCharPointer utf8, int length) {
        try {
            byte[] bytes = readBytes(utf8, length);
            return session(handle).intern(bytes, 0, bytes.length);
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_lookup")
    public static long lookup(IsolateThread thread, VoidPointer handle, CCharPointer utf8, int length) {
        try {
            byte[] bytes = readBytes(utf8, length);
            return session(handle).lookup(bytes, 0, bytes.length);
        } catch (RuntimeException e) {
            return LOOKUP_ERROR;
        }
    }

    @CEntryPoint(name = "nodus_resolve")
    public static int resolve(IsolateThread thread, VoidPointer handle, long id, CCharPointer outBuf, int outCap) {
        try {
            byte[] bytes = session(handle).resolve(id);
            int count = Math.min(bytes.length, Math.max(outCap, 0));
            for (int i = 0; i < count; i++) {
                outBuf.write(i, bytes[i]);
            }
            return bytes.length;
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_add_string_edges_batch")
    public static int addStringEdgesBatch(IsolateThread thread, VoidPointer handle, CCharPointer utf8,
                                          CIntPointer lengths, int pairCount) {
        try {
            if (pairCount < 0) {
                return ERROR;
            }
            int[] lengthValues = intArray(lengths, 2 * pairCount);
            long total = 0;
            for (int length : lengthValues) {
                if (length < 0) {
                    return ERROR;
                }
                total += length;
            }
            byte[] bytes = readBytes(utf8, total);
            return session(handle).addStringEdges(bytes, lengthValues, pairCount);
        } catch (MemoryLimitExceededException e) {
            return MEMORY_LIMIT;
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    private static byte[] readBytes(CCharPointer pointer, long length) {
        if (length < 0 || length > MAX_STRING_BYTES) {
            throw new IllegalArgumentException("string length out of range: " + length);
        }
        byte[] bytes = new byte[(int) length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = pointer.read(i);
        }
        return bytes;
    }

    private static int[] intArray(CIntPointer pointer, int count) {
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = pointer.read(i);
        }
        return values;
    }

    private static GraphSession session(VoidPointer handle) {
        return SESSIONS.get(handle.rawValue());
    }

    private static void report(CIntPointer status, int code) {
        if (status.isNonNull()) {
            status.write(code);
        }
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
