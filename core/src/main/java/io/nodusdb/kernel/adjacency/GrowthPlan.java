package io.nodusdb.kernel.adjacency;

public final class GrowthPlan {

    private final AdditionCounter outgoing = new AdditionCounter();
    private final AdditionCounter incoming = new AdditionCounter();
    private final Headroom outgoingHeadroom = new Headroom();
    private final Headroom incomingHeadroom = new Headroom();

    public void clear() {
        outgoing.clear();
        incoming.clear();
    }

    public void addEdge(int object, int subject) {
        outgoing.add(object);
        incoming.add(subject);
    }

    public long measure(EdgeTables tables) {
        accumulate(outgoing, outgoingHeadroom, tables, true);
        accumulate(incoming, incomingHeadroom, tables, false);
        return tables.bytesToReserve(outgoingHeadroom, incomingHeadroom);
    }

    public void reserve(EdgeTables tables) {
        tables.reserve(outgoingHeadroom, incomingHeadroom);
    }

    private static void accumulate(AdditionCounter counter, Headroom headroom, EdgeTables tables, boolean outgoing) {
        headroom.clear();
        counter.sort();
        for (int index = 0; index < counter.size(); ) {
            int run = counter.runLengthAt(index);
            if (outgoing) {
                tables.accumulateOutgoing(counter.nodeAt(index), run, headroom);
            } else {
                tables.accumulateIncoming(counter.nodeAt(index), run, headroom);
            }
            index += run;
        }
    }
}
