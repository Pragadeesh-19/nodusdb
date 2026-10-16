package io.nodusdb.projection;

public interface ErasedNames {

    record Erased(String pseudonym, long eraseLsn) {
    }

    interface Lookup {

        Erased find(String name);
    }

    Lookup current();
}
