package io.nodusdb.ship;

import java.io.IOException;

public interface SnapshotSource {

    StagedSnapshot stage() throws IOException;
}
