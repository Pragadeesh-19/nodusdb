package io.nodusdb.chain;

import org.junit.jupiter.api.Test;

import static io.nodusdb.chain.ChainFixtures.opaqueRecordsObject;
import static io.nodusdb.chain.ChainFixtures.snapshotRef;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainCursorTest {

    @Test
    void aChainStartsWithASnapshotReferenceAtTheFirstSequence() {
        ChainCursor cursor = ChainCursor.beforeFirst();
        assertTrue(cursor.atStart());

        ChainObject first = snapshotRef(1, ChainHash.ZERO, 1, 100);
        cursor.accept(first);

        assertFalse(cursor.atStart());
        assertEquals(1, cursor.seq());
        assertEquals(first.digest(), cursor.digest());
        assertEquals(1, cursor.epoch());
        assertEquals(100, cursor.lastLsn());
    }

    @Test
    void aChainCannotStartWithRecords() {
        ChainCursor cursor = ChainCursor.beforeFirst();

        ChainTrustException refused = assertThrows(ChainTrustException.class,
                () -> cursor.accept(opaqueRecordsObject(1, ChainHash.ZERO, 1, 1, 5)));

        assertTrue(refused.getMessage().contains("starts with a snapshot reference"), refused.getMessage());
    }

    @Test
    void recordsMustContinueFromTheLastLsn() {
        ChainCursor cursor = ChainCursor.beforeFirst();
        ChainObject ref = snapshotRef(1, ChainHash.ZERO, 1, 100);
        cursor.accept(ref);

        ChainObject first = opaqueRecordsObject(2, ref.digest(), 1, 101, 110);
        cursor.accept(first);
        ChainObject second = opaqueRecordsObject(3, first.digest(), 1, 111, 111);
        cursor.accept(second);

        assertEquals(3, cursor.seq());
        assertEquals(111, cursor.lastLsn());
    }

    @Test
    void aGapOrAnOverlapInLsnsIsRefused() {
        ChainObject ref = snapshotRef(1, ChainHash.ZERO, 1, 100);
        ChainCursor gap = ChainCursor.beforeFirst();
        gap.accept(ref);
        ChainCursor overlap = ChainCursor.beforeFirst();
        overlap.accept(ref);

        assertThrows(ChainTrustException.class, () -> gap.accept(opaqueRecordsObject(2, ref.digest(), 1, 102, 105)));
        assertThrows(ChainTrustException.class, () -> overlap.accept(opaqueRecordsObject(2, ref.digest(), 1, 100, 105)));
    }

    @Test
    void aSequenceGapOrRepeatIsRefused() {
        ChainObject ref = snapshotRef(1, ChainHash.ZERO, 1, 100);
        ChainCursor cursor = ChainCursor.beforeFirst();
        cursor.accept(ref);

        assertThrows(ChainTrustException.class, () -> cursor.accept(opaqueRecordsObject(3, ref.digest(), 1, 101, 101)));
        assertThrows(ChainTrustException.class, () -> cursor.accept(snapshotRef(1, ChainHash.ZERO, 1, 100)));
        assertThrows(ChainTrustException.class, () -> cursor.accept(snapshotRef(5, ref.digest(), 1, 100)));
    }

    @Test
    void anObjectThatDoesNotNameItsPredecessorIsRefused() {
        ChainObject ref = snapshotRef(1, ChainHash.ZERO, 1, 100);
        ChainCursor cursor = ChainCursor.beforeFirst();
        cursor.accept(ref);

        ChainTrustException refused = assertThrows(ChainTrustException.class,
                () -> cursor.accept(opaqueRecordsObject(2, ChainHash.ZERO, 1, 101, 101)));

        assertTrue(refused.getMessage().contains("does not extend"), refused.getMessage());
    }

    @Test
    void anEpochMayStayOrGrowButNeverGoBack() {
        ChainObject ref = snapshotRef(1, ChainHash.ZERO, 5, 100);
        ChainCursor cursor = ChainCursor.beforeFirst();
        cursor.accept(ref);
        ChainObject same = opaqueRecordsObject(2, ref.digest(), 5, 101, 101);
        cursor.accept(same);
        ChainObject higher = opaqueRecordsObject(3, same.digest(), 6, 102, 102);
        cursor.accept(higher);

        assertEquals(6, cursor.epoch());
        assertThrows(ChainTrustException.class,
                () -> cursor.accept(opaqueRecordsObject(4, higher.digest(), 5, 103, 103)));
    }

    @Test
    void aLaterSnapshotReferenceDoesNotMoveTheLsn() {
        ChainObject ref = snapshotRef(1, ChainHash.ZERO, 1, 100);
        ChainCursor cursor = ChainCursor.beforeFirst();
        cursor.accept(ref);
        ChainObject records = opaqueRecordsObject(2, ref.digest(), 1, 101, 150);
        cursor.accept(records);

        ChainObject laterRef = snapshotRef(3, records.digest(), 1, 140);
        cursor.accept(laterRef);

        assertEquals(150, cursor.lastLsn());
        cursor.accept(opaqueRecordsObject(4, laterRef.digest(), 1, 151, 151));
    }

    @Test
    void aFailedAcceptLeavesTheCursorWhereItWas() {
        ChainObject ref = snapshotRef(1, ChainHash.ZERO, 1, 100);
        ChainCursor cursor = ChainCursor.beforeFirst();
        cursor.accept(ref);

        assertThrows(ChainTrustException.class, () -> cursor.accept(opaqueRecordsObject(2, ref.digest(), 1, 105, 110)));

        assertEquals(1, cursor.seq());
        assertEquals(100, cursor.lastLsn());
        assertEquals(ref.digest(), cursor.digest());
        cursor.accept(opaqueRecordsObject(2, ref.digest(), 1, 101, 110));
    }

    @Test
    void aCursorCanResumeAfterAKnownObject() {
        ChainHash digest = ChainHash.sha256(new byte[]{4});
        ChainCursor cursor = ChainCursor.after(40, digest, 3, 900);

        cursor.accept(opaqueRecordsObject(41, digest, 3, 901, 905));

        assertEquals(41, cursor.seq());
        assertEquals(905, cursor.lastLsn());
    }

    @Test
    void aResumedCursorRefusesNonsenseStartingPoints() {
        assertThrows(IllegalArgumentException.class, () -> ChainCursor.after(0, ChainHash.ZERO, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> ChainCursor.after(1, ChainHash.ZERO, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> ChainCursor.after(1, ChainHash.ZERO, 1, -1));
        assertThrows(NullPointerException.class, () -> ChainCursor.after(1, null, 1, 1));
    }
}
