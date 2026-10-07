package io.nodusdb.chain;

import io.nodusdb.objectstore.ObjectKeys;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChainLayoutTest {

    @Test
    void keysArePaddedToTwentyDigitsUnderTheirFolders() {
        assertEquals("_nodus/chain/00000000000000000001.obj", ChainLayout.chainKey(1));
        assertEquals("_nodus/snapshots/00000000000000000042.nsnap", ChainLayout.snapshotKey(42));
        assertEquals("_nodus/epoch/00000000000000000003.json", ChainLayout.epochKey(3));
        assertEquals("_nodus/chain/09223372036854775807.obj", ChainLayout.chainKey(Long.MAX_VALUE));
        assertEquals("_nodus/snapshots/00000000000000000000.nsnap", ChainLayout.snapshotKey(0));
    }

    @Test
    void everyGeneratedKeyIsAValidObjectKey() {
        for (long number : new long[]{0, 1, 99, 1L << 40, Long.MAX_VALUE}) {
            ObjectKeys.requireKey(ChainLayout.chainKey(number));
            ObjectKeys.requireKey(ChainLayout.snapshotKey(number));
            ObjectKeys.requireKey(ChainLayout.epochKey(number));
        }
    }

    @Test
    void parsingReturnsTheNumberTheKeyWasMadeFrom() {
        for (long number : new long[]{0, 1, 7, 123456789012L, Long.MAX_VALUE}) {
            assertEquals(OptionalLong.of(number), ChainLayout.chainSeq(ChainLayout.chainKey(number)));
            assertEquals(OptionalLong.of(number), ChainLayout.snapshotLsn(ChainLayout.snapshotKey(number)));
            assertEquals(OptionalLong.of(number), ChainLayout.epochNumber(ChainLayout.epochKey(number)));
        }
    }

    @Test
    void aKeyOfTheWrongFolderSuffixOrShapeDoesNotParse() {
        String chain = ChainLayout.chainKey(5);

        assertEquals(OptionalLong.empty(), ChainLayout.snapshotLsn(chain));
        assertEquals(OptionalLong.empty(), ChainLayout.epochNumber(chain));
        assertEquals(OptionalLong.empty(), ChainLayout.chainSeq(chain.replace(".obj", ".json")));
        assertEquals(OptionalLong.empty(), ChainLayout.chainSeq("_nodus/chain/5.obj"));
        assertEquals(OptionalLong.empty(), ChainLayout.chainSeq("_nodus/chain/000000000000000000051.obj"));
        assertEquals(OptionalLong.empty(), ChainLayout.chainSeq("_nodus/chain/0000000000000000000x.obj"));
        assertEquals(OptionalLong.empty(), ChainLayout.chainSeq("_nodus/chain/-0000000000000000001.obj"));
        assertEquals(OptionalLong.empty(), ChainLayout.chainSeq("other/00000000000000000005.obj"));
        assertEquals(OptionalLong.empty(), ChainLayout.chainSeq(""));
    }

    @Test
    void aNumberBeyondTheLongRangeDoesNotParse() {
        assertEquals(OptionalLong.empty(), ChainLayout.chainSeq("_nodus/chain/09223372036854775808.obj"));
        assertEquals(OptionalLong.empty(), ChainLayout.chainSeq("_nodus/chain/99999999999999999999.obj"));
    }

    @Test
    void aNegativeNumberCannotBeUsedToMakeAKey() {
        assertThrows(IllegalArgumentException.class, () -> ChainLayout.chainKey(-1));
        assertThrows(IllegalArgumentException.class, () -> ChainLayout.snapshotKey(-1));
        assertThrows(IllegalArgumentException.class, () -> ChainLayout.epochKey(-1));
    }

    @Test
    void sortingKeysAsTextSortsThemByNumber() {
        Random random = new Random(3);
        List<Long> numbers = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            numbers.add(Math.floorMod(random.nextLong(), 1L << (1 + random.nextInt(62))));
        }
        List<String> keys = new ArrayList<>();
        for (long number : numbers) {
            keys.add(ChainLayout.chainKey(number));
        }
        Collections.sort(keys);
        Collections.sort(numbers);

        for (int i = 0; i < keys.size(); i++) {
            assertEquals(numbers.get(i), ChainLayout.chainSeq(keys.get(i)).orElseThrow());
        }
    }

    @Test
    void thePrefixesAreFoldersOfTheirOwn() {
        assertEquals("_nodus/chain/", ChainLayout.CHAIN_PREFIX);
        assertEquals("_nodus/snapshots/", ChainLayout.SNAPSHOT_PREFIX);
        assertEquals("_nodus/epoch/", ChainLayout.EPOCH_PREFIX);
    }
}
