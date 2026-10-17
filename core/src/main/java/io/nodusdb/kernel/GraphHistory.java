package io.nodusdb.kernel;

import io.nodusdb.kernel.past.PastSource;
import io.nodusdb.log.RecordSource;

import java.util.List;
import java.util.Objects;

public record GraphHistory(List<RecordSource> recordSources, PastSource past) {

    public static final GraphHistory NONE = new GraphHistory(List.of(), PastSource.NONE);

    public GraphHistory {
        recordSources = List.copyOf(recordSources);
        Objects.requireNonNull(past, "past");
    }
}
