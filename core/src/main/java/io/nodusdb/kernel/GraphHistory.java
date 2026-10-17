package io.nodusdb.kernel;

import io.nodusdb.log.RecordSource;

import java.util.List;

public interface GraphHistory {

    GraphHistory NONE = List::of;

    List<RecordSource> recordSources();
}
