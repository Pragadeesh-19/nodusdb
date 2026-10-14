package io.nodusdb.authz;

import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.Partition;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.symbols.Pseudonym;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordType;

import java.nio.charset.StandardCharsets;

final class TupleEraser {

    static final int CHUNK_TUPLES = 30_000;

    private final GraphKernel kernel;
    private final int chunkTuples;
    private final RecordBatch batch = new RecordBatch();

    TupleEraser(GraphKernel kernel) {
        this(kernel, CHUNK_TUPLES);
    }

    TupleEraser(GraphKernel kernel, int chunkTuples) {
        if (chunkTuples < 1 || chunkTuples > CHUNK_TUPLES) {
            throw new IllegalArgumentException("a chunk holds 1 to " + CHUNK_TUPLES + " tuples: " + chunkTuples);
        }
        this.kernel = kernel;
        this.chunkTuples = chunkTuples;
    }

    Token erase(String name) {
        if (kernel.keyKind() == KeyKind.INTEGER) {
            throw new UnsupportedFeatureException("the graph is keyed by integers and holds no names to erase");
        }
        byte[] utf8 = name.getBytes(StandardCharsets.UTF_8);
        int node = kernel.symbols().lookup(utf8, 0, utf8.length);
        if (node < 0) {
            return kernel.token();
        }
        if (kernel.catalog().usesSymbol(node)) {
            throw new SchemaViolationException("the name is a type or relation of the schema and cannot be erased");
        }
        byte[] pseudonym = Pseudonym.padded(Pseudonym.of(kernel.erasureSalt(), utf8));
        while (true) {
            batch.clear();
            int collected = collectRemovals(node);
            if (collected < chunkTuples) {
                batch.erase(node, pseudonym);
                batch.commit();
                return new Token(kernel.epoch(), kernel.commit(batch));
            }
            batch.commit();
            kernel.commit(batch);
        }
    }

    private int collectRemovals(int node) {
        int collected = 0;
        for (Partition partition : Partition.values()) {
            if (!kernel.hasPartition(partition)) {
                continue;
            }
            int outgoing = kernel.degree(partition, true, node);
            for (int i = 0; i < outgoing && collected < chunkTuples; i++) {
                long key = kernel.keyAt(partition, true, node, i);
                batch.tuple(RecordType.TUPLE_REMOVE, node, EdgeKey.relation(key), EdgeKey.subjectRelation(key),
                        EdgeKey.id(key));
                collected++;
            }
            int incoming = kernel.degree(partition, false, node);
            for (int i = 0; i < incoming && collected < chunkTuples; i++) {
                long key = kernel.keyAt(partition, false, node, i);
                int object = EdgeKey.id(key);
                if (object != node) {
                    batch.tuple(RecordType.TUPLE_REMOVE, object, EdgeKey.relation(key),
                            EdgeKey.subjectRelation(key), node);
                    collected++;
                }
            }
        }
        return collected;
    }
}
