package io.nodusdb.authz.schema;

import io.nodusdb.authz.schema.SchemaDocument.RelationDef;
import io.nodusdb.authz.schema.SchemaDocument.SubjectRef;
import io.nodusdb.authz.schema.SchemaDocument.Term;
import io.nodusdb.authz.schema.SchemaDocument.TypeDef;
import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.symbols.SymbolTable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

public final class SchemaCompiler {

    private record Entry(int id, int typeSymbol, int nameSymbol, int flags) {
    }

    private final SchemaDocument document;
    private final RelationCatalog current;
    private final SymbolTable symbols;
    private final Map<String, Integer> pendingSymbols = new HashMap<>();
    private final List<String> newSymbols = new ArrayList<>();
    private final Map<String, Integer> priorIds = new HashMap<>();
    private final Map<String, Integer> priorFlags = new HashMap<>();
    private final RelationIds ids = new RelationIds();
    private final List<Entry> entries = new ArrayList<>();
    private int nextId = 1;

    private SchemaCompiler(SchemaDocument document, RelationCatalog current, SymbolTable symbols) {
        this.document = document;
        this.current = current;
        this.symbols = symbols;
    }

    public static SchemaPlan plan(String text, RelationCatalog current, SymbolTable symbols) {
        SchemaDocument document = SchemaParser.parse(text);
        if (document.version() != current.version() + 1) {
            throw new SchemaViolationException("schema version " + document.version() + " does not follow version "
                    + current.version());
        }
        SchemaValidator.validate(document);
        return new SchemaCompiler(document, current, symbols).build();
    }

    public static CompiledSchema rebuild(RelationCatalog catalog, SymbolTable symbols) {
        SchemaDocument document = SchemaParser.parse(new String(catalog.document(), StandardCharsets.UTF_8));
        RelationIds ids = new RelationIds();
        for (int i = 0; i < catalog.relationCount(); i++) {
            if ((catalog.flagsAt(i) & RelationCatalog.RETIRED) == 0) {
                ids.put(nameOf(symbols, catalog.typeSymbolAt(i)), nameOf(symbols, catalog.nameSymbolAt(i)),
                        catalog.idAt(i));
            }
        }
        return CompiledSchema.build(document, ids, typeSymbolsOf(document, symbols));
    }

    private static int[] typeSymbolsOf(SchemaDocument document, SymbolTable symbols) {
        int[] typeSymbols = new int[document.types().size()];
        for (int t = 0; t < typeSymbols.length; t++) {
            typeSymbols[t] = lookup(symbols, document.types().get(t).name());
        }
        return typeSymbols;
    }

    private SchemaPlan build() {
        indexPriorEntries();
        Set<String> declared = new HashSet<>();
        Set<String> memberships = membershipKeys();
        Set<String> tuplesets = tuplesetKeys();
        for (TypeDef type : document.types()) {
            int typeSymbol = symbolFor(type.name());
            for (RelationDef relation : type.relations()) {
                String key = key(type.name(), relation.name());
                declared.add(key);
                int flags = (memberships.contains(key) ? RelationCatalog.MEMBERSHIP : 0)
                        | (tuplesets.contains(key) ? RelationCatalog.TUPLESET : 0);
                int id = idFor(type.name(), relation.name(), key);
                entries.add(new Entry(id, typeSymbol, symbolFor(relation.name()), flags));
                ids.put(type.name(), relation.name(), id);
            }
        }
        retireMissing(declared);
        entries.sort(Comparator.comparingInt(Entry::id));
        byte[] canonical = SchemaWriter.canonicalBytes(document);
        return new SchemaPlan(document, canonical, SchemaWriter.digest(canonical), newSymbols,
                column(Entry::id), column(Entry::typeSymbol), column(Entry::nameSymbol), column(Entry::flags), ids);
    }

    private void indexPriorEntries() {
        for (int i = 0; i < current.relationCount(); i++) {
            String key = key(nameOf(symbols, current.typeSymbolAt(i)), nameOf(symbols, current.nameSymbolAt(i)));
            priorIds.put(key, current.idAt(i));
            priorFlags.put(key, current.flagsAt(i));
            nextId = Math.max(nextId, current.idAt(i) + 1);
        }
    }

    private int idFor(String type, String relation, String key) {
        Integer prior = priorIds.get(key);
        if (prior != null) {
            if ((priorFlags.get(key) & RelationCatalog.RETIRED) != 0) {
                throw new SchemaViolationException("relation '" + type + "#" + relation
                        + "' was retired and cannot be declared again");
            }
            return prior;
        }
        if (nextId > EdgeKey.MAX_RELATION) {
            throw new SchemaViolationException("the schema defines more than " + EdgeKey.MAX_RELATION
                    + " relations over its lifetime");
        }
        return nextId++;
    }

    private void retireMissing(Set<String> declared) {
        for (int i = 0; i < current.relationCount(); i++) {
            String key = key(nameOf(symbols, current.typeSymbolAt(i)), nameOf(symbols, current.nameSymbolAt(i)));
            if (!declared.contains(key)) {
                entries.add(new Entry(current.idAt(i), current.typeSymbolAt(i), current.nameSymbolAt(i),
                        current.flagsAt(i) | RelationCatalog.RETIRED));
            }
        }
    }

    private Set<String> membershipKeys() {
        Set<String> keys = new HashSet<>();
        for (TypeDef type : document.types()) {
            for (RelationDef relation : type.relations()) {
                for (SubjectRef subject : relation.subjects()) {
                    if (subject.isUserset()) {
                        keys.add(key(subject.type(), subject.relation()));
                    }
                }
            }
        }
        return keys;
    }

    private Set<String> tuplesetKeys() {
        Set<String> keys = new HashSet<>();
        for (TypeDef type : document.types()) {
            type.permissions().forEach(permission -> {
                for (Term term : permission.terms()) {
                    if (term.isArrow()) {
                        keys.add(key(type.name(), term.name()));
                    }
                }
            });
        }
        return keys;
    }

    private int symbolFor(String name) {
        int existing = lookup(symbols, name);
        if (existing >= 0) {
            return existing;
        }
        Integer pending = pendingSymbols.get(name);
        if (pending != null) {
            return pending;
        }
        int id = symbols.size() + newSymbols.size();
        pendingSymbols.put(name, id);
        newSymbols.add(name);
        return id;
    }

    private int[] column(ToIntFunction<Entry> field) {
        return entries.stream().mapToInt(field).toArray();
    }

    private static int lookup(SymbolTable symbols, String name) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return symbols.lookup(bytes, 0, bytes.length);
    }

    private static String nameOf(SymbolTable symbols, int symbol) {
        return new String(symbols.resolve(symbol), StandardCharsets.UTF_8);
    }

    private static String key(String type, String relation) {
        return type + '\0' + relation;
    }
}
