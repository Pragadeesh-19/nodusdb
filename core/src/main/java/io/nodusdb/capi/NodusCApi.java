package io.nodusdb.capi;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.VoidPointer;
import org.graalvm.word.WordFactory;

public final class NodusCApi {

    private static final int ERROR = -1;
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
    public static void destroy(IsolateThread thread, VoidPointer handle) {
        try {
            SESSIONS.close(handle.rawValue());
        } catch (RuntimeException e) {
            return;
        }
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
