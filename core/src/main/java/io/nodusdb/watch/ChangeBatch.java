package io.nodusdb.watch;

import io.nodusdb.kernel.Token;

import java.util.List;
import java.util.Objects;

public record ChangeBatch(List<Change> changes, Token next) {

    public ChangeBatch {
        changes = List.copyOf(Objects.requireNonNull(changes, "changes"));
        Objects.requireNonNull(next, "next");
    }

    static ChangeBatch nothingAfter(Token cursor) {
        return new ChangeBatch(List.of(), cursor);
    }
}
