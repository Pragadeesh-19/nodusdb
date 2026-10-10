package io.nodusdb.ship;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLogTest {

    private final EventLog log = new EventLog();

    @Test
    void anEmptyLogHasNoEvents() {
        assertEquals(List.of(), log.recent());
        assertEquals(List.of(), log.drain());
    }

    @Test
    void eventsAreNumberedFromOneInTheOrderTheyWereAdded() {
        log.add("first");
        log.add("second");

        assertEquals(List.of(new EventLog.Event(1, "first"), new EventLog.Event(2, "second")), log.recent());
    }

    @Test
    void readingRecentEventsDoesNotConsumeThem() {
        log.add("first");

        assertEquals(log.recent(), log.recent());
        assertEquals(1, log.recent().size());
    }

    @Test
    void drainingReturnsTheMessagesInOrderAndEmptiesTheLog() {
        log.add("first");
        log.add("second");

        assertEquals(List.of("first", "second"), log.drain());
        assertEquals(List.of(), log.drain());
        assertEquals(List.of(), log.recent());
    }

    @Test
    void numbersKeepCountingAfterADrain() {
        log.add("first");
        log.drain();
        log.add("second");

        assertEquals(List.of(new EventLog.Event(2, "second")), log.recent());
    }

    @Test
    void theLogKeepsTheNewestSixtyFourAndTheNumbersKeepCounting() {
        for (int i = 1; i <= EventLog.MAX_EVENTS + 10; i++) {
            log.add("event " + i);
        }

        List<EventLog.Event> kept = log.recent();

        assertEquals(EventLog.MAX_EVENTS, kept.size());
        assertEquals(new EventLog.Event(11, "event 11"), kept.get(0));
        assertEquals(new EventLog.Event(EventLog.MAX_EVENTS + 10, "event " + (EventLog.MAX_EVENTS + 10)),
                kept.get(kept.size() - 1));
    }

    @Test
    void aReturnedListIsACopy() {
        log.add("first");
        List<EventLog.Event> before = log.recent();

        log.add("second");

        assertEquals(1, before.size());
    }

    @Test
    void concurrentAddersLoseNothingAndKeepTheBound() throws Exception {
        int threads = 6;
        int perThread = 500;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> done = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                done.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        log.add("t" + id + " " + i);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : done) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        List<EventLog.Event> kept = log.recent();

        assertEquals(EventLog.MAX_EVENTS, kept.size());
        assertEquals((long) threads * perThread, kept.get(kept.size() - 1).seq());
        for (int i = 1; i < kept.size(); i++) {
            assertTrue(kept.get(i).seq() == kept.get(i - 1).seq() + 1, "sequence numbers must be contiguous");
        }
    }
}
