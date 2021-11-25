use crunchbase;


INSERT INTO parent_children_orgs
(

(SELECT parent_organizations.parent_uuid AS parent_uuid,
       concat_ws('|', collect_list(organizations.uuid)) AS uuids,
       concat_ws('|', collect_list(organizations.name)) AS names,
       count(organizations.uuid) AS count
FROM parent_organizations
INNER JOIN organizations ON parent_organizations.uuid=organizations.uuid
GROUP BY parent_organizations.parent_uuid)
UNION ALL
(SELECT * FROM parent_children_orgs)

);

INSERT OVERWRITE TABLE parent_children_orgs
SELECT tmp.parent_uuid,
       tmp.names,
       tmp.uuids,
       tmp.count
FROM   (SELECT parent_uuid,
               names,
               uuids,
               count,
               Row_number()
                 OVER ( partition by ID
                   ORDER BY parent_uuid, names, uuids) AS rno
        FROM   parent_children_orgs) as tmp
WHERE  tmp.rno = 1;
