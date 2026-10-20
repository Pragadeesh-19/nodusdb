package io.nodusdb.kernel.memory;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NativeMemoryTest {

    @Test
    void theJvmUsesSegmentAccessors() {
        assertFalse(NativeMemory.usesPointers());
    }

    @Test
    void theSharedContractHoldsOnTheJvm() {
        NativeMemorySelfTest.Result result = NativeMemorySelfTest.run(false);

        assertEquals(0, result.failures(), result.messages());
    }

    @Test
    void theSelfTestReportsAWrongModeAsAFailure() {
        NativeMemorySelfTest.Result result = NativeMemorySelfTest.run(true);

        assertEquals(1, result.failures(), result.messages());
        assertEquals("usesPointers() is false, expected true\n", result.messages());
    }

    @Test
    void anAlignedAccessorRefusesAMisalignedOffsetOnTheJvm() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(64, 64);

            assertThrows(IllegalArgumentException.class, () -> NativeMemory.getLong(segment, 3));
            assertEquals(0, NativeMemory.getLongUnaligned(segment, 3));
        }
    }

    @Test
    void aClosedArenaIsRefused() {
        Arena arena = Arena.ofConfined();
        MemorySegment segment = arena.allocate(64, 64);
        arena.close();

        assertThrows(IllegalStateException.class, () -> NativeMemory.getLong(segment, 0));
    }
}
