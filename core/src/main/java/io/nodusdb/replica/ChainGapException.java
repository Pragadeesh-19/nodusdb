package io.nodusdb.replica;

public final class ChainGapException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final long missingSeq;

    public ChainGapException(long missingSeq) {
        super("chain object " + missingSeq + " is gone but later objects exist; retention deleted what this "
                + "replica still needed");
        this.missingSeq = missingSeq;
    }

    public long missingSeq() {
        return missingSeq;
    }
}
