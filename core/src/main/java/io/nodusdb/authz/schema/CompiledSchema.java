package io.nodusdb.authz.schema;

import io.nodusdb.authz.schema.SchemaDocument.PermissionDef;
import io.nodusdb.authz.schema.SchemaDocument.RelationDef;
import io.nodusdb.authz.schema.SchemaDocument.SubjectRef;
import io.nodusdb.authz.schema.SchemaDocument.Term;
import io.nodusdb.authz.schema.SchemaDocument.TypeDef;
import io.nodusdb.kernel.adjacency.EdgeKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CompiledSchema {

    public static final int NO_DEF = -1;
    public static final int STORED = 0;
    public static final int PERMISSION = 1;
    public static final int COMPUTED = 0;
    public static final int ARROW = 1;

    private static final long[] NO_SUBJECTS = new long[0];

    public static final CompiledSchema EMPTY = build(new SchemaDocument(0, List.of()), new RelationIds(), new int[0]);

    private final int version;
    private final String[] typeNames;
    private final Map<String, Integer> typeIndexByName = new HashMap<>();
    private final int[] typeIndexBySymbol;
    private final String[] defNames;
    private final int[] defTypes;
    private final int[] defKinds;
    private final int[] defRelationIds;
    private final Map<String, Integer> defByQualifiedName = new HashMap<>();
    private final int[] defByRelationId = new int[EdgeKey.MAX_RELATION + 1];
    private final boolean[] tuplesetByRelationId = new boolean[EdgeKey.MAX_RELATION + 1];
    private final int[][] termKinds;
    private final int[][] termFirst;
    private final int[][] termSecond;
    private final List<int[]> arrowTargets = new ArrayList<>();
    private final long[][] allowedSubjects;

    private CompiledSchema(SchemaDocument document, RelationIds ids, int[] typeSymbols) {
        this.version = document.version();
        List<TypeDef> types = document.types();
        this.typeNames = new String[types.size()];
        Arrays.fill(defByRelationId, NO_DEF);
        int defCount = 0;
        int maxSymbol = -1;
        for (int t = 0; t < types.size(); t++) {
            TypeDef type = types.get(t);
            typeNames[t] = type.name();
            typeIndexByName.put(type.name(), t);
            maxSymbol = Math.max(maxSymbol, typeSymbols[t]);
            defCount += type.relations().size() + type.permissions().size();
        }
        this.typeIndexBySymbol = new int[maxSymbol + 1];
        Arrays.fill(typeIndexBySymbol, NO_DEF);
        for (int t = 0; t < types.size(); t++) {
            if (typeSymbols[t] >= 0) {
                typeIndexBySymbol[typeSymbols[t]] = t;
            }
        }
        this.defNames = new String[defCount];
        this.defTypes = new int[defCount];
        this.defKinds = new int[defCount];
        this.defRelationIds = new int[defCount];
        this.termKinds = new int[defCount][];
        this.termFirst = new int[defCount][];
        this.termSecond = new int[defCount][];
        this.allowedSubjects = new long[defCount][];
        declareDefinitions(types, ids);
        defineTerms(types, ids);
        defineAllowedSubjects(types, ids);
    }

    static CompiledSchema build(SchemaDocument document, RelationIds ids, int[] typeSymbols) {
        return new CompiledSchema(document, ids, typeSymbols);
    }

    public int version() {
        return version;
    }

    public int typeCount() {
        return typeNames.length;
    }

    public String typeName(int typeIndex) {
        return typeNames[typeIndex];
    }

    public int typeIndex(String name) {
        return typeIndexByName.getOrDefault(name, NO_DEF);
    }

    public int typeIndexOfSymbol(int symbol) {
        return symbol >= 0 && symbol < typeIndexBySymbol.length ? typeIndexBySymbol[symbol] : NO_DEF;
    }

    public int definition(int typeIndex, String name) {
        return defByQualifiedName.getOrDefault(qualified(typeNames[typeIndex], name), NO_DEF);
    }

    public int definitionCount() {
        return defNames.length;
    }

    public String definitionName(int definition) {
        return defNames[definition];
    }

    public int typeOf(int definition) {
        return defTypes[definition];
    }

    public boolean isStored(int definition) {
        return defKinds[definition] == STORED;
    }

    public int relationIdOf(int definition) {
        return defRelationIds[definition];
    }

    public int definitionOfRelation(int relationId) {
        return relationId >= 0 && relationId < defByRelationId.length ? defByRelationId[relationId] : NO_DEF;
    }

    public boolean isTupleset(int relationId) {
        return relationId >= 0 && relationId < tuplesetByRelationId.length && tuplesetByRelationId[relationId];
    }

    public int termCount(int definition) {
        return termKinds[definition] == null ? 0 : termKinds[definition].length;
    }

    public int termKind(int definition, int term) {
        return termKinds[definition][term];
    }

    public int computedTarget(int definition, int term) {
        return termFirst[definition][term];
    }

    public int tuplesetRelation(int definition, int term) {
        return termFirst[definition][term];
    }

    public int arrowTarget(int definition, int term, int subjectType) {
        return arrowTargets.get(termSecond[definition][term])[subjectType];
    }

    public boolean allows(int definition, int subjectType, int subjectRelationId) {
        long wanted = pack(subjectType, subjectRelationId);
        for (long allowed : allowedSubjects[definition]) {
            if (allowed == wanted) {
                return true;
            }
        }
        return false;
    }

    private void declareDefinitions(List<TypeDef> types, RelationIds ids) {
        int definition = 0;
        for (int t = 0; t < types.size(); t++) {
            TypeDef type = types.get(t);
            for (RelationDef relation : type.relations()) {
                int relationId = ids.idOf(type.name(), relation.name());
                declare(definition, t, STORED, type.name(), relation.name(), relationId);
                defByRelationId[relationId] = definition;
                definition++;
            }
            for (PermissionDef permission : type.permissions()) {
                declare(definition, t, PERMISSION, type.name(), permission.name(), RelationIds.ABSENT);
                definition++;
            }
        }
    }

    private void declare(int definition, int type, int kind, String typeName, String name, int relationId) {
        defNames[definition] = name;
        defTypes[definition] = type;
        defKinds[definition] = kind;
        defRelationIds[definition] = relationId;
        defByQualifiedName.put(qualified(typeName, name), definition);
    }

    private void defineTerms(List<TypeDef> types, RelationIds ids) {
        for (TypeDef type : types) {
            for (PermissionDef permission : type.permissions()) {
                int definition = defByQualifiedName.get(qualified(type.name(), permission.name()));
                int count = permission.terms().size();
                termKinds[definition] = new int[count];
                termFirst[definition] = new int[count];
                termSecond[definition] = new int[count];
                for (int i = 0; i < count; i++) {
                    defineTerm(definition, i, type, permission.terms().get(i), ids);
                }
            }
        }
    }

    private void defineTerm(int definition, int index, TypeDef type, Term term, RelationIds ids) {
        if (!term.isArrow()) {
            termKinds[definition][index] = COMPUTED;
            termFirst[definition][index] = defByQualifiedName.get(qualified(type.name(), term.name()));
            return;
        }
        int relationId = ids.idOf(type.name(), term.name());
        tuplesetByRelationId[relationId] = true;
        termKinds[definition][index] = ARROW;
        termFirst[definition][index] = relationId;
        termSecond[definition][index] = arrowTargets.size();
        arrowTargets.add(targetsOf(type.relation(term.name()), term.target()));
    }

    private int[] targetsOf(RelationDef tupleset, String target) {
        int[] byType = new int[typeNames.length];
        Arrays.fill(byType, NO_DEF);
        for (SubjectRef subject : tupleset.subjects()) {
            int subjectType = typeIndexByName.get(subject.type());
            byType[subjectType] = defByQualifiedName.getOrDefault(qualified(subject.type(), target), NO_DEF);
        }
        return byType;
    }

    private void defineAllowedSubjects(List<TypeDef> types, RelationIds ids) {
        Arrays.fill(allowedSubjects, NO_SUBJECTS);
        for (TypeDef type : types) {
            for (RelationDef relation : type.relations()) {
                int definition = defByQualifiedName.get(qualified(type.name(), relation.name()));
                long[] allowed = new long[relation.subjects().size()];
                for (int i = 0; i < allowed.length; i++) {
                    SubjectRef subject = relation.subjects().get(i);
                    int subjectRelation = subject.isUserset() ? ids.idOf(subject.type(), subject.relation()) : 0;
                    allowed[i] = pack(typeIndexByName.get(subject.type()), subjectRelation);
                }
                allowedSubjects[definition] = allowed;
            }
        }
    }

    private static long pack(int type, int relationId) {
        return ((long) type << Integer.SIZE) | relationId;
    }

    private static String qualified(String type, String name) {
        return type + '#' + name;
    }
}
