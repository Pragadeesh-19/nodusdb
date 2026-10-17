package io.nodusdb.watch;

import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainFetch;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalLong;

final class ChainLocator {

    static final long NOT_HELD = -1;

    private static final int KIND_OFFSET = 6;
    private static final int LSN_FIRST_OFFSET = ChainCodec.HEADER_BYTES;
    private static final int LSN_LAST_OFFSET = ChainCodec.HEADER_BYTES + Long.BYTES;
    private static final int PEEK_BYTES = ChainCodec.HEADER_BYTES + 2 * Long.BYTES;
    private static final int MAX_SKIPPED = 64;
    private static final long NO_LIMIT = Long.MAX_VALUE;

    private record Peek(long seq, long lsnFirst, long lsnLast) {
    }

    private final ObjectStore store;
    private final ChainFetch chain;

    ChainLocator(ObjectStore store, ChainFetch chain) {
        this.store = store;
        this.chain = chain;
    }

    long locate(long targetLsn) {
        OptionalLong oldest = chain.oldestSeq();
        if (oldest.isEmpty()) {
            return NOT_HELD;
        }
        Peek low = recordsAtOrAfter(oldest.getAsLong(), NO_LIMIT);
        if (low == null || low.lsnFirst() > targetLsn) {
            return NOT_HELD;
        }
        long high;
        long step = 1;
        while (true) {
            long probe = low.seq() + step;
            Peek found = recordsAtOrAfter(probe, NO_LIMIT);
            if (found == null || found.lsnFirst() > targetLsn) {
                high = probe;
                break;
            }
            low = found;
            step <<= 1;
        }
        while (high - low.seq() > 1) {
            long middle = (low.seq() + high) >>> 1;
            Peek found = recordsAtOrAfter(middle, high);
            if (found != null && found.lsnFirst() <= targetLsn) {
                low = found;
            } else {
                high = middle;
            }
        }
        return low.lsnLast() >= targetLsn ? low.seq() : low.seq() + 1;
    }

    private Peek recordsAtOrAfter(long from, long limit) {
        long end = Math.min(limit, from + MAX_SKIPPED);
        for (long seq = from; seq < end; seq++) {
            Optional<byte[]> head = store.getRange(ChainLayout.chainKey(seq), 0, PEEK_BYTES);
            if (head.isEmpty()) {
                return null;
            }
            byte[] bytes = head.get();
            if (bytes.length < PEEK_BYTES || (bytes[KIND_OFFSET] & 0xFF) != ChainKind.RECORDS.code()) {
                continue;
            }
            ByteBuffer view = ByteBuffer.wrap(bytes);
            return new Peek(seq, view.getLong(LSN_FIRST_OFFSET), view.getLong(LSN_LAST_OFFSET));
        }
        return null;
    }
}
