package io.nodusdb.iceberg;

public record EdgeLogRow(long lsn, long commitMicros, long txnLsn, long epoch, String event, String objectType,
                         String objectId, String relation, String subjectType, String subjectId,
                         String subjectRelation, int schemaVersion, String detail) {

    public EdgeLogRow withObject(String type, String id) {
        return new EdgeLogRow(lsn, commitMicros, txnLsn, epoch, event, type, id, relation, subjectType, subjectId,
                subjectRelation, schemaVersion, detail);
    }

    public EdgeLogRow withSubject(String type, String id) {
        return new EdgeLogRow(lsn, commitMicros, txnLsn, epoch, event, objectType, objectId, relation, type, id,
                subjectRelation, schemaVersion, detail);
    }

    void addTo(EdgeLogRows rows) {
        rows.add(lsn, commitMicros, txnLsn, epoch, event, objectType, objectId, relation, subjectType, subjectId,
                subjectRelation, schemaVersion, detail);
    }
}
