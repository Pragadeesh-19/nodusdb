package io.nodusdb.authz.schema;

import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.symbols.SymbolTable;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaCompilerTest {

    private final SymbolTable symbols = SchemaFixtures.newSymbols();

    private SchemaPlan plan(String text, RelationCatalog current) {
        return SchemaCompiler.plan(text, current, symbols);
    }

    private RelationCatalog apply(String text, RelationCatalog current) {
        return SchemaFixtures.install(plan(text, current), symbols);
    }

    private int idOf(RelationCatalog catalog, String type, String relation) {
        for (int i = 0; i < catalog.relationCount(); i++) {
            if (name(catalog.typeSymbolAt(i)).equals(type) && name(catalog.nameSymbolAt(i)).equals(relation)) {
                return catalog.idAt(i);
            }
        }
        return 0;
    }

    private String name(int symbol) {
        return new String(symbols.resolve(symbol), StandardCharsets.UTF_8);
    }

    @Test
    void everyDeclaredTypeAndStoredRelationGetsASymbolAndRelationsGetIds() {
        SchemaPlan plan = plan(SchemaFixtures.DOCUMENTS, RelationCatalog.EMPTY);

        assertEquals(1, plan.version());
        assertTrue(plan.newSymbols().containsAll(List.of("user", "group", "folder", "document", "member", "viewer",
                "parent", "editor")));
        assertEquals(new HashSet<>(plan.newSymbols()).size(), plan.newSymbols().size());
        assertEquals(6, plan.relationIds().length);
        assertArrayEquals(new int[] {1, 2, 3, 4, 5, 6}, plan.relationIds());
    }

    @Test
    void flagsMarkUsersetsAndTuplesets() {
        RelationCatalog catalog = apply(SchemaFixtures.DOCUMENTS, RelationCatalog.EMPTY);

        int member = idOf(catalog, "group", "member");
        int folderParent = idOf(catalog, "folder", "parent");
        int documentParent = idOf(catalog, "document", "parent");
        int editor = idOf(catalog, "document", "editor");

        assertEquals(RelationCatalog.MEMBERSHIP, catalog.flagsOf(member));
        assertEquals(RelationCatalog.TUPLESET, catalog.flagsOf(folderParent));
        assertEquals(RelationCatalog.TUPLESET, catalog.flagsOf(documentParent));
        assertEquals(0, catalog.flagsOf(editor));
    }

    @Test
    void theCanonicalDocumentAndItsDigestDoNotDependOnLayout() {
        SchemaPlan tight = plan("schema 1 type a { relation r: a permission p = r }", RelationCatalog.EMPTY);
        SchemaPlan loose = plan("schema 1\n\ntype a {\n  relation  r : a\n  permission p\n = r\n}\n",
                RelationCatalog.EMPTY);

        assertArrayEquals(tight.canonicalDocument(), loose.canonicalDocument());
        assertArrayEquals(tight.digest(), loose.digest());
        assertEquals(32, tight.digest().length);
    }

    @Test
    void aChangedSchemaChangesTheDigest() {
        SchemaPlan first = plan("schema 1 type a { relation r: a }", RelationCatalog.EMPTY);
        SchemaPlan second = plan("schema 1 type a { relation s: a }", RelationCatalog.EMPTY);

        assertFalse(Arrays.equals(first.digest(), second.digest()));
    }

    @Test
    void idsAreStableAcrossVersionsAndNewRelationsGetTheNextFreeId() {
        RelationCatalog v1 = apply("schema 1 type a { relation r: a relation s: a }", RelationCatalog.EMPTY);
        int r = idOf(v1, "a", "r");
        int s = idOf(v1, "a", "s");

        RelationCatalog v2 = apply("schema 2 type a { relation s: a relation r: a relation t: a }", v1);

        assertEquals(r, idOf(v2, "a", "r"));
        assertEquals(s, idOf(v2, "a", "s"));
        assertEquals(Math.max(r, s) + 1, idOf(v2, "a", "t"));
        assertEquals(3, v2.relationCount());
    }

    @Test
    void aRemovedRelationKeepsItsIdAsRetiredAndIsNeverReused() {
        RelationCatalog v1 = apply("schema 1 type a { relation r: a relation s: a }", RelationCatalog.EMPTY);
        int r = idOf(v1, "a", "r");

        RelationCatalog v2 = apply("schema 2 type a { relation s: a }", v1);
        RelationCatalog v3 = apply("schema 3 type a { relation s: a relation u: a }", v2);

        assertTrue(v2.isRetired(r));
        assertTrue(v3.isRetired(r));
        assertNotEquals(r, idOf(v3, "a", "u"));
        assertEquals(3, v3.relationCount());
    }

    @Test
    void aRetiredRelationCannotBeDeclaredAgain() {
        RelationCatalog v1 = apply("schema 1 type a { relation r: a relation s: a }", RelationCatalog.EMPTY);
        RelationCatalog v2 = apply("schema 2 type a { relation s: a }", v1);

        SchemaViolationException failure = assertThrows(SchemaViolationException.class,
                () -> plan("schema 3 type a { relation r: a relation s: a }", v2));

        assertTrue(failure.getMessage().contains("retired"));
    }

    @Test
    void theVersionMustFollowTheCurrentOne() {
        assertThrows(SchemaViolationException.class,
                () -> plan("schema 2 type a { relation r: a }", RelationCatalog.EMPTY));
        RelationCatalog v1 = apply("schema 1 type a { relation r: a }", RelationCatalog.EMPTY);
        assertThrows(SchemaViolationException.class, () -> plan("schema 1 type a { relation r: a }", v1));
        assertThrows(SchemaViolationException.class, () -> plan("schema 3 type a { relation r: a }", v1));
    }

    @Test
    void anInvalidSchemaIsRejectedBeforeAnyIdIsAssigned() {
        assertThrows(SchemaViolationException.class,
                () -> plan("schema 1 type a { relation r: nothing }", RelationCatalog.EMPTY));
    }

    @Test
    void aRebuiltModelMatchesTheOneThatWasPlanned() {
        RelationCatalog catalog = apply(SchemaFixtures.DOCUMENTS, RelationCatalog.EMPTY);

        CompiledSchema model = SchemaCompiler.rebuild(catalog, symbols);

        assertEquals(1, model.version());
        assertEquals(4, model.typeCount());
        int document = model.typeIndex("document");
        int viewer = model.definition(document, "viewer");
        int view = model.definition(document, "view");
        assertTrue(model.isStored(viewer));
        assertFalse(model.isStored(view));
        assertEquals(idOf(catalog, "document", "viewer"), model.relationIdOf(viewer));
        assertEquals(viewer, model.definitionOfRelation(idOf(catalog, "document", "viewer")));
        assertEquals(3, model.termCount(view));
        assertEquals(CompiledSchema.COMPUTED, model.termKind(view, 0));
        assertEquals(viewer, model.computedTarget(view, 0));
        assertEquals(CompiledSchema.ARROW, model.termKind(view, 2));
        assertTrue(model.isTupleset(idOf(catalog, "document", "parent")));
        assertFalse(model.isTupleset(idOf(catalog, "document", "viewer")));
        int folder = model.typeIndex("folder");
        assertEquals(model.definition(folder, "view"), model.arrowTarget(view, 2, folder));
        assertEquals(CompiledSchema.NO_DEF, model.arrowTarget(view, 2, document));
    }

    @Test
    void theModelKnowsWhichSubjectsEachRelationAllows() {
        RelationCatalog catalog = apply(SchemaFixtures.DOCUMENTS, RelationCatalog.EMPTY);
        CompiledSchema model = SchemaCompiler.rebuild(catalog, symbols);
        int document = model.typeIndex("document");
        int editor = model.definition(document, "editor");
        int user = model.typeIndex("user");
        int group = model.typeIndex("group");
        int member = idOf(catalog, "group", "member");

        assertTrue(model.allows(editor, user, 0));
        assertTrue(model.allows(editor, group, member));
        assertFalse(model.allows(editor, group, 0));
        assertFalse(model.allows(editor, document, 0));
    }

    @Test
    void typesAreFoundByTheirSymbol() {
        RelationCatalog catalog = apply(SchemaFixtures.DOCUMENTS, RelationCatalog.EMPTY);
        CompiledSchema model = SchemaCompiler.rebuild(catalog, symbols);
        byte[] folder = "folder".getBytes(StandardCharsets.UTF_8);

        assertEquals(model.typeIndex("folder"), model.typeIndexOfSymbol(symbols.lookup(folder, 0, folder.length)));
        assertEquals(CompiledSchema.NO_DEF, model.typeIndexOfSymbol(9_999));
        assertEquals(CompiledSchema.NO_DEF, model.typeIndexOfSymbol(-1));
    }

    @Test
    void theEmptyModelHasNoTypes() {
        assertEquals(0, CompiledSchema.EMPTY.version());
        assertEquals(0, CompiledSchema.EMPTY.typeCount());
        assertEquals(CompiledSchema.NO_DEF, CompiledSchema.EMPTY.typeIndex("user"));
    }
}
