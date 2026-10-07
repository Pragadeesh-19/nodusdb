package io.nodusdb.authz.schema;

import io.nodusdb.error.SchemaViolationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaValidatorTest {

    private static String problem(String source) {
        SchemaDocument document = SchemaParser.parse(source);
        return assertThrows(SchemaViolationException.class, () -> SchemaValidator.validate(document)).getMessage();
    }

    @Test
    void theExampleSchemaIsValid() {
        assertDoesNotThrow(() -> SchemaValidator.validate(SchemaParser.parse(SchemaFixtures.DOCUMENTS)));
    }

    @Test
    void aTypeCannotBeDefinedTwice() {
        assertTrue(problem("schema 1 type a type a").contains("defined twice"));
    }

    @Test
    void aMemberNameIsUniqueAcrossRelationsAndPermissions() {
        assertTrue(problem("schema 1 type a { relation r: a relation r: a }").contains("'r' is defined twice"));
        assertTrue(problem("schema 1 type a { relation r: a permission r = r }").contains("'r' is defined twice"));
    }

    @Test
    void subjectTypesAndUsersetRelationsMustExist() {
        assertTrue(problem("schema 1 type a { relation r: missing }").contains("unknown type 'missing'"));
        assertTrue(problem("schema 1 type a { relation r: a#nope }").contains("not a relation of that type"));
        assertTrue(problem("schema 1 type a { relation r: a permission p = r relation s: a#p }")
                .contains("not a relation of that type"));
    }

    @Test
    void aSubjectTypeCannotBeListedTwice() {
        assertTrue(problem("schema 1 type a { relation r: a | a }").contains("lists a subject type twice"));
    }

    @Test
    void permissionTermsMustBeDefinedOnTheSameType() {
        assertTrue(problem("schema 1 type a { permission p = nothing }").contains("does not define"));
    }

    @Test
    void anArrowFollowsARelationWithPlainSubjectsThatDefineTheTarget() {
        assertTrue(problem("schema 1 type a { permission p = x->y }").contains("not a relation of the type"));
        assertTrue(problem("schema 1 type a { relation r: a#r permission p = r->p }")
                .contains("lists a userset subject"));
        assertTrue(problem("schema 1 type b type a { relation r: b permission p = r->q }")
                .contains("type 'b' does not define it"));
        assertDoesNotThrow(() -> SchemaValidator.validate(
                SchemaParser.parse("schema 1 type b { relation v: b } type a { relation r: b permission p = r->v }")));
    }

    @Test
    void permissionsThatOnlyReferToEachOtherAreACycle() {
        assertTrue(problem("schema 1 type a { permission p = q permission q = p }").contains("depends on itself"));
        assertTrue(problem("schema 1 type a { permission p = p }").contains("depends on itself"));
    }

    @Test
    void recursionThroughAStoredRelationIsNotACycle() {
        assertDoesNotThrow(() -> SchemaValidator.validate(SchemaParser.parse(
                "schema 1 type a { relation parent: a permission view = parent->view }")));
    }
}
