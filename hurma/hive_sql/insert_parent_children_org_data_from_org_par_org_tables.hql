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
