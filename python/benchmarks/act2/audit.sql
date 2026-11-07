WITH RECURSIVE
changes AS (
    SELECT txn_lsn, lsn, event, object_type, object_id, relation, subject_type, subject_id, subject_relation
    FROM iceberg_scan($metadata)
    WHERE event IN ('add', 'remove') AND txn_lsn <= $lsn
),
tuples AS (
    SELECT object_type, object_id, relation, subject_type, subject_id, subject_relation
    FROM changes
    GROUP BY object_type, object_id, relation, subject_type, subject_id, subject_relation
    HAVING arg_max(event, lsn) = 'add'
),
members (group_id, user_id) AS (
    SELECT object_id, subject_id
    FROM tuples
    WHERE object_type = 'group' AND relation = 'member' AND subject_type = 'user' AND subject_relation = ''
    UNION
    SELECT nested.object_id, members.user_id
    FROM tuples AS nested
    JOIN members ON nested.subject_id = members.group_id
    WHERE nested.object_type = 'group' AND nested.relation = 'member'
      AND nested.subject_type = 'group' AND nested.subject_relation = 'member'
),
ancestors (object_type, object_id) AS (
    SELECT $object_type, $object_id
    UNION
    SELECT parent.subject_type, parent.subject_id
    FROM tuples AS parent
    JOIN ancestors ON parent.object_type = ancestors.object_type AND parent.object_id = ancestors.object_id
    WHERE parent.relation = 'parent'
),
grants AS (
    SELECT viewer.subject_type, viewer.subject_id, viewer.subject_relation
    FROM tuples AS viewer
    JOIN ancestors ON viewer.object_type = ancestors.object_type AND viewer.object_id = ancestors.object_id
    WHERE viewer.relation = 'viewer'
)
SELECT 'user:' || subject_id AS viewer
FROM grants
WHERE subject_type = 'user' AND subject_relation = ''
UNION
SELECT 'user:' || members.user_id
FROM grants
JOIN members ON grants.subject_id = members.group_id
WHERE grants.subject_type = 'group' AND grants.subject_relation = 'member'
ORDER BY viewer
