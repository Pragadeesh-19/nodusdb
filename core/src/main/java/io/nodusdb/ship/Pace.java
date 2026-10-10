package io.nodusdb.ship;

public record Pace(Kind kind, long nanos) {

    public enum Kind {
        CONTINUE, IDLE, BACKOFF, STOP
    }

    private static final Pace CONTINUE = new Pace(Kind.CONTINUE, 0);
    private static final Pace STOP = new Pace(Kind.STOP, 0);

    public Pace {
        if (nanos < 0) {
            throw new IllegalArgumentException("a pause is not negative: " + nanos);
        }
    }

    public static Pace immediately() {
        return CONTINUE;
    }

    public static Pace stop() {
        return STOP;
    }

    public static Pace idle(long nanos) {
        return new Pace(Kind.IDLE, nanos);
    }

    public static Pace backoff(long nanos) {
        return new Pace(Kind.BACKOFF, nanos);
    }
}
