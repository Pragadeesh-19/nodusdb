package io.nodusdb.authz.schema;

import java.util.List;

public record SchemaDocument(int version, List<TypeDef> types) {

    public record SubjectRef(String type, String relation) {

        public boolean isUserset() {
            return relation != null;
        }
    }

    public record RelationDef(String name, List<SubjectRef> subjects) {
    }

    public record Term(String name, String target) {

        public boolean isArrow() {
            return target != null;
        }
    }

    public record PermissionDef(String name, List<Term> terms) {
    }

    public record TypeDef(String name, List<RelationDef> relations, List<PermissionDef> permissions) {

        public RelationDef relation(String relationName) {
            for (RelationDef relation : relations) {
                if (relation.name().equals(relationName)) {
                    return relation;
                }
            }
            return null;
        }

        public PermissionDef permission(String permissionName) {
            for (PermissionDef permission : permissions) {
                if (permission.name().equals(permissionName)) {
                    return permission;
                }
            }
            return null;
        }

        public boolean defines(String memberName) {
            return relation(memberName) != null || permission(memberName) != null;
        }
    }

    public SchemaDocument {
        types = List.copyOf(types);
    }

    public TypeDef type(String typeName) {
        for (TypeDef type : types) {
            if (type.name().equals(typeName)) {
                return type;
            }
        }
        return null;
    }
}
