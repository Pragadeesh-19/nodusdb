package io.nodusdb.log.simulation;

import io.nodusdb.log.io.LogChannel;
import io.nodusdb.log.io.LogFileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class SimulatedDisk implements LogFileSystem {

    public enum Kind { CREATE, WRITE, TRUNCATE, FORCE, DELETE }

    public record Event(Kind kind, String file, long position, byte[] data, long size) {
    }

    public static final long ALL_PENDING = -1L;
    public static final long NO_PENDING = 0L;

    private final List<Event> events = new ArrayList<>();
    private final Map<String, byte[]> files = new TreeMap<>();
    private String failForcesOn;

    public synchronized int eventCount() {
        return events.size();
    }

    public synchronized List<Event> events() {
        return List.copyOf(events);
    }

    public synchronized void failForcesOn(String fileNameFragment) {
        failForcesOn = fileNameFragment;
    }

    public synchronized byte[] contentOf(String name) {
        return files.get(name).clone();
    }

    public synchronized void replaceContent(String name, byte[] content) {
        files.put(name, content.clone());
    }

    public synchronized SimulatedDisk crashImage(int eventsKept, long pendingMask) {
        return crashImage(eventsKept, pendingMask, -1);
    }

    public synchronized SimulatedDisk crashImage(int eventsKept, long pendingMask, int lastWritePrefix) {
        Map<String, byte[]> durable = new HashMap<>();
        Map<String, List<Event>> pending = new HashMap<>();
        for (int i = 0; i < eventsKept; i++) {
            Event event = tornIfLast(events.get(i), i == eventsKept - 1, lastWritePrefix);
            switch (event.kind()) {
                case CREATE -> {
                    durable.put(event.file(), new byte[0]);
                    pending.put(event.file(), new ArrayList<>());
                }
                case WRITE, TRUNCATE -> pending.get(event.file()).add(event);
                case FORCE -> {
                    durable.put(event.file(), apply(durable.get(event.file()), pending.get(event.file()), ALL_PENDING));
                    pending.get(event.file()).clear();
                }
                case DELETE -> {
                    durable.remove(event.file());
                    pending.remove(event.file());
                }
            }
        }
        SimulatedDisk image = new SimulatedDisk();
        for (Map.Entry<String, byte[]> entry : durable.entrySet()) {
            byte[] content = apply(entry.getValue(), pending.get(entry.getKey()), pendingMask);
            image.files.put(entry.getKey(), content);
        }
        return image;
    }

    public synchronized int sizeOfWrite(int eventIndex) {
        Event event = events.get(eventIndex);
        return event.kind() == Kind.WRITE ? event.data().length : -1;
    }

    private static Event tornIfLast(Event event, boolean isLast, int prefix) {
        if (!isLast || prefix < 0 || event.kind() != Kind.WRITE || prefix >= event.data().length) {
            return event;
        }
        return new Event(Kind.WRITE, event.file(), event.position(), Arrays.copyOf(event.data(), prefix), prefix);
    }

    public synchronized int pendingWritesAfter(int eventsKept) {
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < eventsKept; i++) {
            Event event = events.get(i);
            switch (event.kind()) {
                case WRITE, TRUNCATE -> counts.merge(event.file(), 1, Integer::sum);
                case FORCE, DELETE -> counts.remove(event.file());
                case CREATE -> counts.put(event.file(), 0);
            }
        }
        return counts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
    }

    private static byte[] apply(byte[] base, List<Event> pending, long mask) {
        byte[] content = base;
        for (int index = 0; index < pending.size(); index++) {
            boolean selected = mask == ALL_PENDING || (index < Long.SIZE && (mask >>> index & 1L) != 0);
            if (!selected) {
                continue;
            }
            Event event = pending.get(index);
            if (event.kind() == Kind.TRUNCATE) {
                content = event.size() < content.length ? Arrays.copyOf(content, (int) event.size()) : content;
            } else {
                int end = (int) event.position() + event.data().length;
                if (end > content.length) {
                    content = Arrays.copyOf(content, end);
                }
                System.arraycopy(event.data(), 0, content, (int) event.position(), event.data().length);
            }
        }
        return content;
    }

    @Override
    public synchronized List<String> list() {
        return new ArrayList<>(files.keySet());
    }

    @Override
    public synchronized boolean exists(String name) {
        return files.containsKey(name);
    }

    @Override
    public synchronized LogChannel create(String name) throws IOException {
        if (files.containsKey(name)) {
            throw new IOException("file already exists: " + name);
        }
        files.put(name, new byte[0]);
        events.add(new Event(Kind.CREATE, name, 0, null, 0));
        return new Channel(name);
    }

    @Override
    public synchronized LogChannel open(String name) throws IOException {
        if (!files.containsKey(name)) {
            throw new FileNotFoundException(name);
        }
        return new Channel(name);
    }

    @Override
    public synchronized void delete(String name) {
        if (files.remove(name) != null) {
            events.add(new Event(Kind.DELETE, name, 0, null, 0));
        }
    }

    @Override
    public void trySyncDirectory() {
    }

    private final class Channel implements LogChannel {

        private final String name;

        Channel(String name) {
            this.name = name;
        }

        @Override
        public int read(ByteBuffer destination, long position) {
            synchronized (SimulatedDisk.this) {
                byte[] content = files.get(name);
                if (position >= content.length) {
                    return -1;
                }
                int count = (int) Math.min(destination.remaining(), content.length - position);
                destination.put(content, (int) position, count);
                return count;
            }
        }

        @Override
        public int write(ByteBuffer source, long position) {
            synchronized (SimulatedDisk.this) {
                byte[] data = new byte[source.remaining()];
                source.get(data);
                byte[] content = files.get(name);
                int end = (int) position + data.length;
                if (end > content.length) {
                    content = Arrays.copyOf(content, end);
                    files.put(name, content);
                }
                System.arraycopy(data, 0, content, (int) position, data.length);
                events.add(new Event(Kind.WRITE, name, position, data, data.length));
                return data.length;
            }
        }

        @Override
        public long size() {
            synchronized (SimulatedDisk.this) {
                return files.get(name).length;
            }
        }

        @Override
        public void truncate(long size) {
            synchronized (SimulatedDisk.this) {
                byte[] content = files.get(name);
                if (size < content.length) {
                    files.put(name, Arrays.copyOf(content, (int) size));
                }
                events.add(new Event(Kind.TRUNCATE, name, 0, null, size));
            }
        }

        @Override
        public void force() throws IOException {
            synchronized (SimulatedDisk.this) {
                if (failForcesOn != null && name.contains(failForcesOn)) {
                    throw new IOException("simulated force failure on " + name);
                }
                events.add(new Event(Kind.FORCE, name, 0, null, 0));
            }
        }

        @Override
        public void close() {
        }
    }
}
