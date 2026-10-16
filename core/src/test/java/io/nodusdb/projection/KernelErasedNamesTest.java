package io.nodusdb.projection;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.kernel.ErasedSymbols;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.symbols.Pseudonym;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class KernelErasedNamesTest {

    private final GraphKernel kernel = new GraphKernel();
    private final TupleStore store = TupleStore.open(kernel);

    private String pseudonym(String name) {
        return new String(Pseudonym.of(kernel.erasureSalt(), name.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.US_ASCII);
    }

    @Test
    void anErasedNameIsFoundWithTheLsnOfItsErasureAndOtherNamesAreNot() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        store.add("document:readme", "viewer", "user:alice");
        store.add("document:readme", "viewer", "user:bob");
        Token erased = store.erase("user:alice");

        ErasedNames.Lookup lookup = new KernelErasedNames(kernel).current();

        assertEquals(new ErasedNames.Erased(pseudonym("user:alice"), erased.lsn()), lookup.find("user:alice"));
        assertNull(lookup.find("user:bob"));
        assertNull(lookup.find(pseudonym("user:alice")));
    }

    @Test
    void theKernelRecordsEachErasureWithItsCommitLsn() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        store.add("document:readme", "viewer", "user:alice");
        store.add("document:readme", "viewer", "user:bob");
        Token first = store.erase("user:alice");
        Token second = store.erase("user:bob");

        ErasedSymbols log = kernel.erasedSymbolLog();

        assertEquals(2, log.count());
        assertEquals(first.lsn(), log.eraseLsn(0));
        assertEquals(second.lsn(), log.eraseLsn(1));
        assertEquals(second.lsn(), kernel.newestEraseLsn());
    }

    @Test
    void aNameErasedTwiceIsFoundWithItsNewestErasure() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        store.add("document:readme", "viewer", "user:alice");
        store.erase("user:alice");
        store.add("document:plan", "viewer", "user:alice");
        Token again = store.erase("user:alice");

        ErasedNames.Erased found = new KernelErasedNames(kernel).current().find("user:alice");

        assertEquals(again.lsn(), found.eraseLsn());
    }
}
