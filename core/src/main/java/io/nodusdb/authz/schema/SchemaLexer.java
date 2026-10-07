package io.nodusdb.authz.schema;

import io.nodusdb.error.SchemaViolationException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;

final class SchemaLexer {

    enum Kind {
        IDENT,
        NUMBER,
        LEFT_BRACE,
        RIGHT_BRACE,
        COLON,
        PIPE,
        EQUALS,
        PLUS,
        ARROW,
        HASH,
        END
    }

    record Token(Kind kind, String text, int line) {

        String describe() {
            return kind == Kind.END ? "the end of the schema" : "'" + text + "'";
        }
    }

    private final String source;
    private final List<Token> tokens = new ArrayList<>();
    private int position;
    private int line = 1;

    private SchemaLexer(String source) {
        this.source = source;
    }

    static List<Token> tokenize(String source) {
        SchemaLexer lexer = new SchemaLexer(source);
        lexer.scan();
        return lexer.tokens;
    }

    private void scan() {
        while (position < source.length()) {
            char c = source.charAt(position);
            if (c == '\n') {
                line++;
                position++;
            } else if (Character.isWhitespace(c)) {
                position++;
            } else if (c == '/' && peek(1) == '/') {
                skipComment();
            } else if (isIdentifierStart(c)) {
                tokens.add(new Token(Kind.IDENT, read(SchemaLexer::isIdentifierPart), line));
            } else if (c >= '0' && c <= '9') {
                tokens.add(new Token(Kind.NUMBER, read(ch -> ch >= '0' && ch <= '9'), line));
            } else {
                scanSymbol(c);
            }
        }
        tokens.add(new Token(Kind.END, "", line));
    }

    private void scanSymbol(char c) {
        switch (c) {
            case '{' -> single(Kind.LEFT_BRACE, c);
            case '}' -> single(Kind.RIGHT_BRACE, c);
            case ':' -> single(Kind.COLON, c);
            case '|' -> single(Kind.PIPE, c);
            case '=' -> single(Kind.EQUALS, c);
            case '+' -> single(Kind.PLUS, c);
            case '#' -> single(Kind.HASH, c);
            case '-' -> scanDash();
            case '&' -> throw reserved('&');
            default -> throw new SchemaViolationException("line " + line + ": unexpected character '" + c + "'");
        }
    }

    private void scanDash() {
        if (peek(1) == '>') {
            tokens.add(new Token(Kind.ARROW, "->", line));
            position += 2;
            return;
        }
        throw reserved('-');
    }

    private SchemaViolationException reserved(char operator) {
        return new SchemaViolationException("line " + line + ": the operator '" + operator
                + "' is reserved and not supported in this schema version");
    }

    private void single(Kind kind, char c) {
        tokens.add(new Token(kind, String.valueOf(c), line));
        position++;
    }

    private void skipComment() {
        while (position < source.length() && source.charAt(position) != '\n') {
            position++;
        }
    }

    private String read(IntPredicate accepted) {
        int start = position;
        while (position < source.length() && accepted.test(source.charAt(position))) {
            position++;
        }
        return source.substring(start, position);
    }

    private char peek(int offset) {
        int index = position + offset;
        return index < source.length() ? source.charAt(index) : '\0';
    }

    private static boolean isIdentifierStart(int c) {
        return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    private static boolean isIdentifierPart(int c) {
        return isIdentifierStart(c) || (c >= '0' && c <= '9');
    }
}
