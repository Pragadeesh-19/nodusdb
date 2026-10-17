package io.nodusdb.log;

import java.io.IOException;
import java.util.Optional;

public interface RecordSource {

    String name();

    Optional<RecordStream> open(long afterLsn) throws IOException;
}
