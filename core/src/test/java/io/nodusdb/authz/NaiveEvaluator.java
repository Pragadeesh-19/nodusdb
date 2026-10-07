package io.nodusdb.authz;

import io.nodusdb.authz.schema.SchemaDocument;
import io.nodusdb.authz.schema.SchemaDocument.PermissionDef;
import io.nodusdb.authz.schema.SchemaDocument.RelationDef;
import io.nodusdb.authz.schema.SchemaDocument.Term;
import io.nodusdb.authz.schema.SchemaDocument.TypeDef;

import java.util.HashSet;
import java.util.Set;

final class NaiveEvaluator {

    record Tuple(String object, String relation, String subject, String subjectRelation) {
    }

    private final SchemaDocument schema;
    private final Set<Tuple> tuples;

    NaiveEvaluator(SchemaDocument schema, Set<Tuple> tuples) {
        this.schema = schema;
        this.tuples = tuples;
    }

    boolean check(String object, String name, String subject) {
        return holds(object, name, subject, new HashSet<>());
    }

    private boolean holds(String object, String name, String subject, Set<String> visited) {
        if (!visited.add(object + '|' + name)) {
            return false;
        }
        TypeDef type = schema.type(object.substring(0, object.indexOf(':')));
        RelationDef relation = type.relation(name);
        if (relation != null) {
            return relationHolds(object, name, subject, visited);
        }
        PermissionDef permission = type.permission(name);
        for (Term term : permission.terms()) {
            if (term.isArrow() ? arrowHolds(object, term, subject, visited)
                    : holds(object, term.name(), subject, visited)) {
                return true;
            }
        }
        return false;
    }

    private boolean relationHolds(String object, String name, String subject, Set<String> visited) {
        if (tuples.contains(new Tuple(object, name, subject, null))) {
            return true;
        }
        for (Tuple tuple : tuples) {
            if (tuple.object().equals(object) && tuple.relation().equals(name) && tuple.subjectRelation() != null
                    && holds(tuple.subject(), tuple.subjectRelation(), subject, visited)) {
                return true;
            }
        }
        return false;
    }

    private boolean arrowHolds(String object, Term term, String subject, Set<String> visited) {
        for (Tuple tuple : tuples) {
            if (tuple.object().equals(object) && tuple.relation().equals(term.name())
                    && tuple.subjectRelation() == null) {
                String childType = tuple.subject().substring(0, tuple.subject().indexOf(':'));
                if (schema.type(childType).defines(term.target())
                        && holds(tuple.subject(), term.target(), subject, visited)) {
                    return true;
                }
            }
        }
        return false;
    }
}
