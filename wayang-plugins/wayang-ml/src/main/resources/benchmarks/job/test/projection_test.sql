SELECT ct.id AS leftID,
       ct2.id as rightID
FROM postgres.company_type AS ct
INNER JOIN postgres.company_type AS ct2 ON ct.id = ct2.id;
