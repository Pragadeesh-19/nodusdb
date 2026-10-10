package io.nodusdb.replica;

import io.nodusdb.json.JsonWriter;
import io.nodusdb.storage.GraphSurvey;

import java.util.List;

public record SalvageReport(long handoffLsn, boolean provisional, long localLastLsn, long unavailableThroughLsn,
                            List<GraphSurvey.Transaction> transactions) {

    public String toJson() {
        JsonWriter json = new JsonWriter();
        json.beginObject();
        json.name("handoff_lsn").value(handoffLsn);
        json.name("provisional").value(provisional);
        json.name("local_last_lsn").value(localLastLsn);
        json.name("unavailable_through_lsn").value(unavailableThroughLsn);
        json.name("transactions").beginArray();
        for (GraphSurvey.Transaction transaction : transactions) {
            json.beginObject();
            json.name("first_lsn").value(transaction.firstLsn());
            json.name("last_lsn").value(transaction.lastLsn());
            json.name("commit_micros").value(transaction.commitMicros());
            json.name("changes").beginArray();
            for (GraphSurvey.Change change : transaction.changes()) {
                json.beginObject();
                json.name("kind").value(change.kind());
                json.name("object").value(change.object());
                json.name("relation").value(change.relation());
                json.name("subject").value(change.subject());
                json.name("subject_relation").value(change.subjectRelation());
                json.endObject();
            }
            json.endArray();
            json.endObject();
        }
        json.endArray();
        json.endObject();
        return json.toString();
    }
}
