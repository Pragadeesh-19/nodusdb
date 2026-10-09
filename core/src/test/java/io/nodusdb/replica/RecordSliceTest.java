package io.nodusdb.replica;

import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.ship.ChainBuilder;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordSliceTest {

    private final byte[] first = ChainBuilder.transaction(1, 2);
    private final byte[] second = ChainBuilder.transaction(4, 2);
    private final byte[] third = ChainBuilder.transaction(7, 2);
    private final byte[] all = concat(first, second, third);

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    @Test
    void startingAtOrBeforeTheFirstRecordKeepsEverything() {
        assertArrayEquals(all, RecordSlice.from(all, 1));
        assertArrayEquals(all, RecordSlice.from(all, 0));
        assertArrayEquals(all, RecordSlice.from(all, -5));
    }

    @Test
    void startingAtATransactionBoundaryDropsTheEarlierTransactions() {
        assertArrayEquals(concat(second, third), RecordSlice.from(all, 4));
        assertArrayEquals(third, RecordSlice.from(all, 7));
    }

    @Test
    void startingPastTheLastRecordLeavesNothing() {
        assertEquals(0, RecordSlice.from(all, 10).length);
        assertEquals(0, RecordSlice.from(all, 1_000).length);
    }

    @Test
    void startingInsideATransactionIsRefused() {
        for (long lsn : new long[]{2, 3, 5, 6, 8, 9}) {
            ChainTrustException refused = assertThrows(ChainTrustException.class, () -> RecordSlice.from(all, lsn));
            assertTrue(refused.getMessage().contains("inside a transaction"), refused.getMessage());
        }
    }

    @Test
    void theSliceIsACopyAndLeavesTheInputUntouched() {
        byte[] before = Arrays.copyOf(all, all.length);

        byte[] slice = RecordSlice.from(all, 4);
        slice[0] ^= 0x7F;

        assertArrayEquals(before, all);
    }

    @Test
    void aSingleTransactionObjectCanBeSlicedAtItsStartOnly() {
        assertArrayEquals(second, RecordSlice.from(second, 4));
        assertEquals(0, RecordSlice.from(second, 7).length);
        assertThrows(ChainTrustException.class, () -> RecordSlice.from(second, 5));
    }
}
