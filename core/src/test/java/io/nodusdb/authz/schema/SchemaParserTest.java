package io.nodusdb.authz.schema;

import io.nodusdb.authz.schema.SchemaDocument.PermissionDef;
import io.nodusdb.authz.schema.SchemaDocument.TypeDef;
import io.nodusdb.error.SchemaViolationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaParserTest {

    private static String messageOf(String source) {
        return assertThrows(SchemaViolationException.class, () -> SchemaParser.parse(source)).getMessage();
    }

    @Test
    void theExampleSchemaParsesIntoItsTypesRelationsAndPermissions() {
        SchemaDocument document = SchemaParser.parse(SchemaFixtures.DOCUMENTS);

        assertEquals(1, document.version());
        assertEquals(4, document.types().size());
        TypeDef documentType = document.type("document");
        assertEquals(3, documentType.relations().size());
        assertEquals("editor", documentType.relations().get(1).name());
        assertEquals(2, documentType.relation("editor").subjects().size());
        assertTrue(documentType.relation("editor").subjects().get(1).isUserset());
        assertEquals("member", documentType.relation("editor").subjects().get(1).relation());
        PermissionDef view = documentType.permission("view");
        assertEquals(3, view.terms().size());
        assertFalse(view.terms().get(0).isArrow());
        assertTrue(view.terms().get(2).isArrow());
        assertEquals("parent", view.terms().get(2).name());
        assertEquals("view", view.terms().get(2).target());
    }

    @Test
    void aTypeMayHaveNoBodyOrAnEmptyBody() {
        SchemaDocument document = SchemaParser.parse("schema 3 type a type b { } type c { relation r: a }");

        assertEquals(3, document.version());
        assertTrue(document.type("a").relations().isEmpty());
        assertTrue(document.type("b").permissions().isEmpty());
        assertEquals(1, document.type("c").relations().size());
    }

    @Test
    void commentsAndLayoutAreIgnored() {
        SchemaDocument tight = SchemaParser.parse("schema 1 type a { relation r: a | a#r permission p = r }");
        SchemaDocument loose = SchemaParser.parse("""
                // header
                schema 1

                type a {   // trailing
                    relation   r :  a
                       | a # r
                    permission p
                        = r
                }
                """);

        assertEquals(SchemaWriter.canonical(tight), SchemaWriter.canonical(loose));
    }

    @Test
    void theHeaderIsRequired() {
        assertTrue(messageOf("type a").contains("expected 'schema'"));
        assertTrue(messageOf("schema type a").contains("expected a number"));
        assertTrue(messageOf("schema 0").contains("starts at 1"));
        assertTrue(messageOf("schema 99999999999").contains("too large"));
    }

    @Test
    void theReservedOperatorsAreRejectedWithTheirLine() {
        String intersection = messageOf("schema 1\ntype a {\n relation r: a\n permission p = r & r\n}");
        String exclusion = messageOf("schema 1\ntype a {\n relation r: a\n permission p = r - r\n}");

        assertTrue(intersection.startsWith("line 4:") && intersection.contains("'&' is reserved"), intersection);
        assertTrue(exclusion.startsWith("line 4:") && exclusion.contains("'-' is reserved"), exclusion);
    }

    @Test
    void keywordsCannotBeNames() {
        assertTrue(messageOf("schema 1 type relation").contains("keyword"));
        assertTrue(messageOf("schema 1 type a { relation permission: a }").contains("keyword"));
    }

    @Test
    void syntaxErrorsNameTheTokenThatWasFound() {
        assertTrue(messageOf("schema 1 type a { relation r a }").contains("expected ':'"));
        assertTrue(messageOf("schema 1 type a { relation r: }").contains("expected a subject type"));
        assertTrue(messageOf("schema 1 type a { permission p r }").contains("expected '='"));
        assertTrue(messageOf("schema 1 type a { permission p = r -> }").contains("permission reached through r"));
        assertTrue(messageOf("schema 1 type a { thing x }").contains("expected 'relation' or 'permission'"));
        assertTrue(messageOf("schema 1 type a {").contains("the end of the schema"));
        assertTrue(messageOf("schema 1 type a ?").contains("unexpected character '?'"));
    }
}
