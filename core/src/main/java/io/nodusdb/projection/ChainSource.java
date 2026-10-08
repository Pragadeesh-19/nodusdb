package io.nodusdb.projection;

import io.nodusdb.chain.ChainObject;

import java.util.Optional;
import java.util.OptionalLong;

public interface ChainSource {

    Optional<ChainObject> fetch(long seq);

    OptionalLong oldestSeq();
}
