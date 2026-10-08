package io.nodusdb.projection;

import io.nodusdb.iceberg.EdgeLogRows;
import io.nodusdb.iceberg.NodusLogTable;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

final class DayBuffers {

    private final TreeMap<Integer, EdgeLogRows> byDay = new TreeMap<>();
    private long rows;

    void add(long commitMicros, long lsn, long txnLsn, long epoch, String event, String objectType, String objectId,
             String relation, String subjectType, String subjectId, String subjectRelation, int schemaVersion,
             String detail) {
        int day = NodusLogTable.dayOf(commitMicros);
        byDay.computeIfAbsent(day, ignored -> new EdgeLogRows()).add(lsn, commitMicros, txnLsn, epoch, event,
                objectType, objectId, relation, subjectType, subjectId, subjectRelation, schemaVersion, detail);
        rows++;
    }

    long rows() {
        return rows;
    }

    boolean any() {
        return rows > 0;
    }

    List<Integer> daysWithAtLeast(int minimum) {
        List<Integer> days = new ArrayList<>();
        byDay.forEach((day, buffer) -> {
            if (buffer.size() >= minimum) {
                days.add(day);
            }
        });
        return days;
    }

    List<Integer> nonEmptyDays() {
        return daysWithAtLeast(1);
    }

    EdgeLogRows buffer(int day) {
        return byDay.get(day);
    }

    void written(int day) {
        EdgeLogRows buffer = byDay.get(day);
        rows -= buffer.size();
        buffer.clear();
    }
}
