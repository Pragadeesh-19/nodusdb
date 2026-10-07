package io.nodusdb.authz.schema;

import io.nodusdb.authz.schema.SchemaDocument.PermissionDef;
import io.nodusdb.authz.schema.SchemaDocument.RelationDef;
import io.nodusdb.authz.schema.SchemaDocument.SubjectRef;
import io.nodusdb.authz.schema.SchemaDocument.Term;
import io.nodusdb.authz.schema.SchemaDocument.TypeDef;
import io.nodusdb.authz.schema.SchemaLexer.Kind;
import io.nodusdb.authz.schema.SchemaLexer.Token;
import io.nodusdb.error.SchemaViolationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class SchemaParser {

    private static final Set<String> KEYWORDS = Set.of("schema", "type", "relation", "permission");
    private static final String SCHEMA = "schema";
    private static final String TYPE = "type";
    private static final String RELATION = "relation";
    private static final String PERMISSION = "permission";

    private final List<Token> tokens;
    private int cursor;

    private SchemaParser(List<Token> tokens) {
        this.tokens = tokens;
    }

    public static SchemaDocument parse(String source) {
        return new SchemaParser(SchemaLexer.tokenize(source)).document();
    }

    private SchemaDocument document() {
        expectKeyword(SCHEMA);
        Token number = expect(Kind.NUMBER);
        int version = parseVersion(number);
        List<TypeDef> types = new ArrayList<>();
        while (peek().kind() != Kind.END) {
            types.add(typeDefinition());
        }
        return new SchemaDocument(version, types);
    }

    private TypeDef typeDefinition() {
        expectKeyword(TYPE);
        String name = name("a type name");
        List<RelationDef> relations = new ArrayList<>();
        List<PermissionDef> permissions = new ArrayList<>();
        if (accept(Kind.LEFT_BRACE)) {
            while (!accept(Kind.RIGHT_BRACE)) {
                member(relations, permissions);
            }
        }
        return new TypeDef(name, relations, permissions);
    }

    private void member(List<RelationDef> relations, List<PermissionDef> permissions) {
        Token head = expect(Kind.IDENT);
        if (head.text().equals(RELATION)) {
            relations.add(relationDefinition());
        } else if (head.text().equals(PERMISSION)) {
            permissions.add(permissionDefinition());
        } else {
            throw error(head, "expected 'relation' or 'permission' but found " + head.describe());
        }
    }

    private RelationDef relationDefinition() {
        String name = name("a relation name");
        expect(Kind.COLON);
        List<SubjectRef> subjects = new ArrayList<>();
        subjects.add(subjectReference());
        while (accept(Kind.PIPE)) {
            subjects.add(subjectReference());
        }
        return new RelationDef(name, subjects);
    }

    private SubjectRef subjectReference() {
        String type = name("a subject type");
        String relation = accept(Kind.HASH) ? name("a subject relation") : null;
        return new SubjectRef(type, relation);
    }

    private PermissionDef permissionDefinition() {
        String name = name("a permission name");
        expect(Kind.EQUALS);
        List<Term> terms = new ArrayList<>();
        terms.add(term());
        while (accept(Kind.PLUS)) {
            terms.add(term());
        }
        return new PermissionDef(name, terms);
    }

    private Term term() {
        String first = name("a relation or permission name");
        if (accept(Kind.ARROW)) {
            return new Term(first, name("the permission reached through " + first));
        }
        return new Term(first, null);
    }

    private String name(String what) {
        Token token = peek();
        if (token.kind() != Kind.IDENT) {
            throw error(token, "expected " + what + " but found " + token.describe());
        }
        if (KEYWORDS.contains(token.text())) {
            throw error(token, "'" + token.text() + "' is a keyword and cannot be used as " + what);
        }
        cursor++;
        return token.text();
    }

    private void expectKeyword(String keyword) {
        Token token = peek();
        if (token.kind() != Kind.IDENT || !token.text().equals(keyword)) {
            throw error(token, "expected '" + keyword + "' but found " + token.describe());
        }
        cursor++;
    }

    private Token expect(Kind kind) {
        Token token = peek();
        if (token.kind() != kind) {
            throw error(token, "expected " + describe(kind) + " but found " + token.describe());
        }
        cursor++;
        return token;
    }

    private boolean accept(Kind kind) {
        if (peek().kind() == kind) {
            cursor++;
            return true;
        }
        return false;
    }

    private Token peek() {
        return tokens.get(cursor);
    }

    private static int parseVersion(Token number) {
        try {
            int version = Integer.parseInt(number.text());
            if (version < 1) {
                throw error(number, "the schema version starts at 1");
            }
            return version;
        } catch (NumberFormatException e) {
            throw error(number, "the schema version is too large: " + number.text());
        }
    }

    private static String describe(Kind kind) {
        return switch (kind) {
            case IDENT -> "a name";
            case NUMBER -> "a number";
            case LEFT_BRACE -> "'{'";
            case RIGHT_BRACE -> "'}'";
            case COLON -> "':'";
            case PIPE -> "'|'";
            case EQUALS -> "'='";
            case PLUS -> "'+'";
            case ARROW -> "'->'";
            case HASH -> "'#'";
            case END -> "the end of the schema";
        };
    }

    private static SchemaViolationException error(Token at, String message) {
        return new SchemaViolationException("line " + at.line() + ": " + message);
    }
}
