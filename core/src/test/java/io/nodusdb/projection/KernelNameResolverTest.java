package io.nodusdb.projection;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.catalog.RelationCatalog;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KernelNameResolverTest {

    private final GraphKernel kernel = new GraphKernel();
    private final TupleStore store = TupleStore.open(kernel);
    private final KernelNameResolver names = new KernelNameResolver(kernel);

    private int symbolId(String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        return kernel.symbols().lookup(bytes, 0, bytes.length);
    }

    @Test
    void symbolsResolveToTheTextTheyWereInternedWith() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        store.add("document:readme", "viewer", "user:alice");

        assertEquals("document:readme", names.symbol(symbolId("document:readme")));
        assertEquals("user:alice", names.symbol(symbolId("user:alice")));
    }

    @Test
    void anUnknownOrNegativeSymbolIsMarkedNotThrown() {
        assertEquals("#0", names.symbol(0));
        assertEquals("#99", names.symbol(99));
        assertEquals("#-1", names.symbol(-1));
    }

    @Test
    void relationsResolveToTheirNamesFromTheCatalog() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        RelationCatalog catalog = kernel.catalog();

        Set<String> resolved = new HashSet<>();
        for (int i = 0; i < catalog.relationCount(); i++) {
            resolved.add(names.relation(catalog.idAt(i)));
        }

        assertTrue(resolved.containsAll(Set.of("member", "viewer", "editor", "parent")), resolved.toString());
    }

    @Test
    void anUnknownRelationIsMarked() {
        store.applySchema(SchemaFixtures.DOCUMENTS);

        assertEquals("#60000", names.relation(60_000));
    }

    @Test
    void aNewSchemaVersionIsPickedUpWithoutRestarting() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        int before = names.schemaVersion();
        assertEquals("#7777", names.relation(7_777));

        store.applySchema(SchemaFixtures.DOCUMENTS.replace("schema 1", "schema 2")
                + "type label {\n  relation owner: user\n}\n");

        assertEquals(before + 1, names.schemaVersion());
        RelationCatalog catalog = kernel.catalog();
        Set<String> resolved = new HashSet<>();
        for (int i = 0; i < catalog.relationCount(); i++) {
            resolved.add(names.relation(catalog.idAt(i)));
        }
        assertTrue(resolved.contains("owner"), resolved.toString());
    }

    @Test
    void theSchemaVersionIsZeroBeforeAnySchema() {
        assertEquals(0, names.schemaVersion());
    }

    @Test
    void theTenureEpochIsThatOfTheLatestTenureStartingAtOrBeforeTheLsn() {
        kernel.epochHistory().record(1, 1, 0);
        kernel.epochHistory().record(2, 50, 49);
        kernel.epochHistory().record(5, 120, 119);

        assertEquals(0, names.tenureEpoch(0));
        assertEquals(1, names.tenureEpoch(1));
        assertEquals(1, names.tenureEpoch(49));
        assertEquals(2, names.tenureEpoch(50));
        assertEquals(2, names.tenureEpoch(119));
        assertEquals(5, names.tenureEpoch(120));
        assertEquals(5, names.tenureEpoch(1_000_000));
    }

    @Test
    void withoutAnyTenureTheEpochIsZero() {
        assertEquals(0, names.tenureEpoch(10));
    }
}
