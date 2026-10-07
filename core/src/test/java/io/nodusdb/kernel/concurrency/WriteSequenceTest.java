package io.nodusdb.kernel.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteSequenceTest {

    @Test
    void aReadSurvivesWhenNoWriteHappenedAndFailsWhenOneDid() {
        WriteSequence sequence = new WriteSequence();

        long untouched = sequence.beginRead();
        assertTrue(sequence.validate(untouched));

        long overlapped = sequence.beginRead();
        sequence.beginWrite();
        sequence.endWrite();
        assertFalse(sequence.validate(overlapped));
    }

    @Test
    void aWriterThatSetsTwoFieldsInOneSectionIsNeverObservedHalfDone() throws InterruptedException {
        WriteSequence sequence = new WriteSequence();
        long[] pair = new long[2];
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong torn = new AtomicLong();
        AtomicLong checked = new AtomicLong();
        Thread writer = new Thread(() -> {
            for (long i = 1; !stop.get(); i++) {
                sequence.beginWrite();
                pair[0] = i;
                pair[1] = i;
                sequence.endWrite();
            }
        });
        Thread[] readers = new Thread[4];
        for (int r = 0; r < readers.length; r++) {
            readers[r] = new Thread(() -> {
                while (!stop.get()) {
                    long started = sequence.beginRead();
                    long a = pair[0];
                    long b = pair[1];
                    if (sequence.validate(started)) {
                        checked.incrementAndGet();
                        if (a != b) {
                            torn.incrementAndGet();
                        }
                    }
                }
            });
        }
        writer.start();
        for (Thread reader : readers) {
            reader.start();
        }
        Thread.sleep(500);
        stop.set(true);
        writer.join();
        for (Thread reader : readers) {
            reader.join();
        }

        assertEquals(0, torn.get());
        assertTrue(checked.get() > 0);
    }
}
