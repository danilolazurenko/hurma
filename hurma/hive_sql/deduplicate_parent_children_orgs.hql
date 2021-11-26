use crunchbase;

set hive.exec.dynamic.partition=true
set hive.exec.dynamic.partition.mode=nonstrict

INSERT OVERWRITE TABLE parent_children_orgs
SELECT t.parent_uuid,
       t.names,
       t.uuids,
       t.count
FROM   (SELECT parent_uuid,
               names,
               uuids,
               count,
               Row_number()
                 OVER ( partition by parent_uuid
                   ORDER BY parent_uuid, names, uuids) AS rno
        FROM   parent_children_orgs) as t
WHERE  t.rno = 1;
