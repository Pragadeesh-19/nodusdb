package io.nodusdb.authz;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class TupleTransaction {

    public record Operation(boolean add, String object, String relation, String subject, String subjectRelation) {

        public Operation {
            Objects.requireNonNull(object, "object");
            Objects.requireNonNull(relation, "relation");
            Objects.requireNonNull(subject, "subject");
        }
    }

    private final List<Operation> operations = new ArrayList<>();

    public TupleTransaction add(String object, String relation, String subject) {
        return add(object, relation, subject, null);
    }

    public TupleTransaction add(String object, String relation, String subject, String subjectRelation) {
        operations.add(new Operation(true, object, relation, subject, subjectRelation));
        return this;
    }

    public TupleTransaction remove(String object, String relation, String subject) {
        return remove(object, relation, subject, null);
    }

    public TupleTransaction remove(String object, String relation, String subject, String subjectRelation) {
        operations.add(new Operation(false, object, relation, subject, subjectRelation));
        return this;
    }

    public boolean isEmpty() {
        return operations.isEmpty();
    }

    public int size() {
        return operations.size();
    }

    public List<Operation> operations() {
        return Collections.unmodifiableList(operations);
    }
}
