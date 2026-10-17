package io.nodusdb.watch;

import io.nodusdb.kernel.Token;

import java.util.Objects;

public record Change(ChangeKind kind, long lsn, Token token, long commitMicros, String object, String relation,
                     String subject, String subjectRelation, String detail, int schemaVersion) {

    public static final int NO_SCHEMA_VERSION = 0;

    public Change {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(relation, "relation");
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(subjectRelation, "subjectRelation");
        Objects.requireNonNull(detail, "detail");
        if (lsn < 1 || lsn > token.lsn()) {
            throw new IllegalArgumentException("change at LSN " + lsn + " does not belong to the commit " + token);
        }
    }
}
