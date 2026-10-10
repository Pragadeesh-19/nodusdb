package io.nodusdb.ship;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public final class EventLog {

    public record Event(long seq, String message) {
    }

    public static final int MAX_EVENTS = 64;

    private final Deque<Event> events = new ArrayDeque<>();
    private long sequence;

    public synchronized void add(String message) {
        if (events.size() == MAX_EVENTS) {
            events.removeFirst();
        }
        events.addLast(new Event(++sequence, message));
    }

    public synchronized List<String> drain() {
        List<String> drained = new ArrayList<>(events.size());
        for (Event event : events) {
            drained.add(event.message());
        }
        events.clear();
        return drained;
    }

    public synchronized List<Event> recent() {
        return List.copyOf(events);
    }
}
