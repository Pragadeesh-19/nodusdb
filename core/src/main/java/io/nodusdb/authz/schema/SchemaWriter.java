package io.nodusdb.authz.schema;

import io.nodusdb.authz.schema.SchemaDocument.PermissionDef;
import io.nodusdb.authz.schema.SchemaDocument.RelationDef;
import io.nodusdb.authz.schema.SchemaDocument.SubjectRef;
import io.nodusdb.authz.schema.SchemaDocument.Term;
import io.nodusdb.authz.schema.SchemaDocument.TypeDef;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.StringJoiner;

public final class SchemaWriter {

    private static final String INDENT = "  ";

    private SchemaWriter() {
    }

    public static String canonical(SchemaDocument document) {
        StringBuilder out = new StringBuilder("schema ").append(document.version()).append('\n');
        for (TypeDef type : document.types()) {
            writeType(out, type);
        }
        return out.toString();
    }

    public static byte[] canonicalBytes(SchemaDocument document) {
        return canonical(document).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] digest(byte[] canonicalBytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(canonicalBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static void writeType(StringBuilder out, TypeDef type) {
        out.append("type ").append(type.name());
        if (type.relations().isEmpty() && type.permissions().isEmpty()) {
            out.append('\n');
            return;
        }
        out.append(" {\n");
        for (RelationDef relation : type.relations()) {
            out.append(INDENT).append("relation ").append(relation.name()).append(": ")
                    .append(subjects(relation)).append('\n');
        }
        for (PermissionDef permission : type.permissions()) {
            out.append(INDENT).append("permission ").append(permission.name()).append(" = ")
                    .append(terms(permission)).append('\n');
        }
        out.append("}\n");
    }

    private static String subjects(RelationDef relation) {
        StringJoiner joined = new StringJoiner(" | ");
        for (SubjectRef subject : relation.subjects()) {
            joined.add(subject.isUserset() ? subject.type() + "#" + subject.relation() : subject.type());
        }
        return joined.toString();
    }

    private static String terms(PermissionDef permission) {
        StringJoiner joined = new StringJoiner(" + ");
        for (Term term : permission.terms()) {
            joined.add(term.isArrow() ? term.name() + "->" + term.target() : term.name());
        }
        return joined.toString();
    }
}
