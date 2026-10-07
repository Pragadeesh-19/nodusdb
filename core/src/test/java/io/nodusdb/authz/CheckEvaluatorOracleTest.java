package io.nodusdb.authz;

import io.nodusdb.authz.NaiveEvaluator.Tuple;
import io.nodusdb.authz.schema.SchemaDocument;
import io.nodusdb.authz.schema.SchemaDocument.PermissionDef;
import io.nodusdb.authz.schema.SchemaDocument.RelationDef;
import io.nodusdb.authz.schema.SchemaDocument.SubjectRef;
import io.nodusdb.authz.schema.SchemaDocument.TypeDef;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.authz.schema.SchemaParser;
import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckEvaluatorOracleTest {

    private static final int OBJECTS_PER_TYPE = 6;
    private static final int OPERATIONS = 400;
    private static final int QUERIES = 600;
    private static final int UNBOUNDED_DEPTH = 10_000;

    private record Question(String object, String name, String subject) {
    }

    private static String node(Random random, String type) {
        return type + ":" + random.nextInt(OBJECTS_PER_TYPE);
    }

    private static Tuple randomTuple(Random random, SchemaDocument schema) {
        TypeDef type = schema.types().get(random.nextInt(schema.types().size()));
        while (type.relations().isEmpty()) {
            type = schema.types().get(random.nextInt(schema.types().size()));
        }
        RelationDef relation = type.relations().get(random.nextInt(type.relations().size()));
        SubjectRef subject = relation.subjects().get(random.nextInt(relation.subjects().size()));
        return new Tuple(node(random, type.name()), relation.name(), node(random, subject.type()),
                subject.relation());
    }

    private static List<Question> allQuestions(Random random, SchemaDocument schema) {
        List<Question> questions = new ArrayList<>();
        for (int i = 0; i < QUERIES; i++) {
            TypeDef type = schema.types().get(random.nextInt(schema.types().size()));
            List<String> names = new ArrayList<>();
            type.relations().forEach(relation -> names.add(relation.name()));
            type.permissions().forEach((PermissionDef permission) -> names.add(permission.name()));
            if (names.isEmpty()) {
                continue;
            }
            questions.add(new Question(node(random, type.name()), names.get(random.nextInt(names.size())),
                    node(random, "user")));
        }
        return questions;
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    void theIterativeEvaluatorAgreesWithTheNaiveRecursiveOne(long seed) {
        SchemaDocument schema = SchemaParser.parse(SchemaFixtures.DOCUMENTS);
        TupleStore store = TupleStore.open(new GraphKernel(), UNBOUNDED_DEPTH);
        store.applySchema(SchemaFixtures.DOCUMENTS);
        Set<Tuple> oracle = new HashSet<>();
        Random random = new Random(seed);

        for (int op = 0; op < OPERATIONS; op++) {
            Tuple tuple = randomTuple(random, schema);
            boolean add = random.nextInt(4) != 0;
            TupleTransaction transaction = new TupleTransaction();
            if (add) {
                transaction.add(tuple.object(), tuple.relation(), tuple.subject(), tuple.subjectRelation());
                oracle.add(tuple);
            } else {
                transaction.remove(tuple.object(), tuple.relation(), tuple.subject(), tuple.subjectRelation());
                oracle.remove(tuple);
            }
            store.write(transaction);
        }

        NaiveEvaluator naive = new NaiveEvaluator(schema, oracle);
        for (Question question : allQuestions(random, schema)) {
            boolean expected = naive.check(question.object(), question.name(), question.subject());
            assertEquals(expected, store.check(question.object(), question.name(), question.subject()),
                    "seed=" + seed + " " + question);
        }
    }

    @Test
    void theOracleCoversGrantedAndDeniedAnswers() {
        SchemaDocument schema = SchemaParser.parse(SchemaFixtures.DOCUMENTS);
        TupleStore store = TupleStore.open(new GraphKernel(), UNBOUNDED_DEPTH);
        store.applySchema(SchemaFixtures.DOCUMENTS);
        Set<Tuple> oracle = new HashSet<>();
        Random random = new Random(99);
        for (int op = 0; op < 1_500; op++) {
            Tuple tuple = randomTuple(random, schema);
            oracle.add(tuple);
            store.write(new TupleTransaction().add(tuple.object(), tuple.relation(), tuple.subject(),
                    tuple.subjectRelation()));
        }
        NaiveEvaluator naive = new NaiveEvaluator(schema, oracle);
        int granted = 0;
        int denied = 0;
        for (Question question : allQuestions(random, schema)) {
            if (naive.check(question.object(), question.name(), question.subject())) {
                granted++;
            } else {
                denied++;
            }
        }

        assertTrue(granted > 20 && denied > 20, "granted=" + granted + " denied=" + denied);
    }
}
