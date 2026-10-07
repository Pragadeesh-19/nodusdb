package io.nodusdb.authz.schema;

import io.nodusdb.authz.schema.SchemaDocument.PermissionDef;
import io.nodusdb.authz.schema.SchemaDocument.RelationDef;
import io.nodusdb.authz.schema.SchemaDocument.SubjectRef;
import io.nodusdb.authz.schema.SchemaDocument.Term;
import io.nodusdb.authz.schema.SchemaDocument.TypeDef;
import io.nodusdb.error.SchemaViolationException;

import java.util.HashSet;
import java.util.Set;

public final class SchemaValidator {

    private final SchemaDocument document;

    private SchemaValidator(SchemaDocument document) {
        this.document = document;
    }

    public static void validate(SchemaDocument document) {
        new SchemaValidator(document).run();
    }

    private void run() {
        requireUniqueTypes();
        for (TypeDef type : document.types()) {
            requireUniqueMembers(type);
            for (RelationDef relation : type.relations()) {
                checkSubjects(type, relation);
            }
        }
        for (TypeDef type : document.types()) {
            for (PermissionDef permission : type.permissions()) {
                checkTerms(type, permission);
            }
            rejectComputedCycles(type);
        }
    }

    private void requireUniqueTypes() {
        Set<String> seen = new HashSet<>();
        for (TypeDef type : document.types()) {
            if (!seen.add(type.name())) {
                throw violation(type.name(), "the type is defined twice");
            }
        }
    }

    private static void requireUniqueMembers(TypeDef type) {
        Set<String> seen = new HashSet<>();
        for (RelationDef relation : type.relations()) {
            if (!seen.add(relation.name())) {
                throw violation(type.name(), "'" + relation.name() + "' is defined twice");
            }
        }
        for (PermissionDef permission : type.permissions()) {
            if (!seen.add(permission.name())) {
                throw violation(type.name(), "'" + permission.name() + "' is defined twice");
            }
        }
    }

    private void checkSubjects(TypeDef type, RelationDef relation) {
        Set<SubjectRef> seen = new HashSet<>();
        for (SubjectRef subject : relation.subjects()) {
            if (!seen.add(subject)) {
                throw violation(type.name(), "relation '" + relation.name() + "' lists a subject type twice");
            }
            TypeDef subjectType = document.type(subject.type());
            if (subjectType == null) {
                throw violation(type.name(), "relation '" + relation.name() + "' names the unknown type '"
                        + subject.type() + "'");
            }
            if (subject.isUserset() && subjectType.relation(subject.relation()) == null) {
                throw violation(type.name(), "relation '" + relation.name() + "' names '" + subject.type() + "#"
                        + subject.relation() + "', which is not a relation of that type");
            }
        }
    }

    private void checkTerms(TypeDef type, PermissionDef permission) {
        for (Term term : permission.terms()) {
            if (term.isArrow()) {
                checkArrow(type, permission, term);
            } else if (!type.defines(term.name())) {
                throw violation(type.name(), "permission '" + permission.name() + "' uses '" + term.name()
                        + "', which the type does not define");
            }
        }
    }

    private void checkArrow(TypeDef type, PermissionDef permission, Term term) {
        RelationDef tupleset = type.relation(term.name());
        if (tupleset == null) {
            throw violation(type.name(), "permission '" + permission.name() + "' follows '" + term.name()
                    + "', which is not a relation of the type");
        }
        for (SubjectRef subject : tupleset.subjects()) {
            if (subject.isUserset()) {
                throw violation(type.name(), "permission '" + permission.name() + "' follows '" + term.name()
                        + "', which lists a userset subject");
            }
            if (!document.type(subject.type()).defines(term.target())) {
                throw violation(type.name(), "permission '" + permission.name() + "' reaches '" + term.target()
                        + "' through '" + term.name() + "', but type '" + subject.type() + "' does not define it");
            }
        }
    }

    private static void rejectComputedCycles(TypeDef type) {
        for (PermissionDef start : type.permissions()) {
            Set<String> path = new HashSet<>();
            visit(type, start, path);
        }
    }

    private static void visit(TypeDef type, PermissionDef permission, Set<String> path) {
        if (!path.add(permission.name())) {
            throw violation(type.name(), "permission '" + permission.name()
                    + "' depends on itself without passing through a stored relation");
        }
        for (Term term : permission.terms()) {
            PermissionDef next = term.isArrow() ? null : type.permission(term.name());
            if (next != null) {
                visit(type, next, path);
            }
        }
        path.remove(permission.name());
    }

    private static SchemaViolationException violation(String typeName, String message) {
        return new SchemaViolationException("type '" + typeName + "': " + message);
    }
}
