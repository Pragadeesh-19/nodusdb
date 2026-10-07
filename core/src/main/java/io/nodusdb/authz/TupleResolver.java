package io.nodusdb.authz;

import io.nodusdb.authz.TupleTransaction.Operation;
import io.nodusdb.authz.schema.CompiledSchema;
import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.kernel.symbols.SymbolTable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class TupleResolver {

    record Resolved(boolean add, int object, int relation, int subjectRelation, int subject) {
    }

    record Resolution(int firstNewSymbol, List<byte[]> newSymbols, List<Resolved> tuples) {
    }

    private static final int ABSENT = -1;

    private final CompiledSchema model;
    private final SymbolTable symbols;
    private final Map<String, Integer> fresh = new HashMap<>();
    private final List<byte[]> newSymbols = new ArrayList<>();

    TupleResolver(CompiledSchema model, SymbolTable symbols) {
        this.model = model;
        this.symbols = symbols;
    }

    Resolution resolve(List<Operation> operations) {
        ObjectNames.requireSchema(model);
        int first = symbols.size();
        List<Resolved> tuples = new ArrayList<>(operations.size());
        for (Operation operation : operations) {
            Resolved resolved = resolveOne(operation);
            if (resolved != null) {
                tuples.add(resolved);
            }
        }
        return new Resolution(first, newSymbols, tuples);
    }

    private Resolved resolveOne(Operation operation) {
        int objectType = ObjectNames.typeIndex(model, operation.object());
        int definition = model.definition(objectType, operation.relation());
        if (definition == CompiledSchema.NO_DEF) {
            throw new SchemaViolationException("type '" + model.typeName(objectType) + "' has no relation '"
                    + operation.relation() + "'");
        }
        if (!model.isStored(definition)) {
            throw new SchemaViolationException("'" + operation.relation()
                    + "' is a permission, and only relations hold tuples");
        }
        int subjectType = ObjectNames.typeIndex(model, operation.subject());
        int subjectRelation = subjectRelationId(operation, subjectType);
        if (!model.allows(definition, subjectType, subjectRelation)) {
            throw new SchemaViolationException("relation '" + model.typeName(objectType) + "#"
                    + operation.relation() + "' does not allow the subject '" + operation.subject()
                    + (operation.subjectRelation() == null ? "" : "#" + operation.subjectRelation()) + "'");
        }
        int object = node(operation.object(), operation.add());
        int subject = node(operation.subject(), operation.add());
        if (object == ABSENT || subject == ABSENT) {
            return null;
        }
        return new Resolved(operation.add(), object, model.relationIdOf(definition), subjectRelation, subject);
    }

    private int subjectRelationId(Operation operation, int subjectType) {
        if (operation.subjectRelation() == null) {
            return 0;
        }
        int definition = model.definition(subjectType, operation.subjectRelation());
        if (definition == CompiledSchema.NO_DEF || !model.isStored(definition)) {
            throw new SchemaViolationException("type '" + model.typeName(subjectType) + "' has no relation '"
                    + operation.subjectRelation() + "'");
        }
        return model.relationIdOf(definition);
    }

    private int node(String name, boolean create) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        int existing = symbols.lookup(bytes, 0, bytes.length);
        if (existing >= 0) {
            return existing;
        }
        Integer pending = fresh.get(name);
        if (pending != null) {
            return pending;
        }
        if (!create) {
            return ABSENT;
        }
        int id = symbols.size() + newSymbols.size();
        fresh.put(name, id);
        newSymbols.add(bytes);
        return id;
    }
}
