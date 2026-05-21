SELECT
    mc.note AS production_note,
    ct.id as company_type_id,
    t.id as title_id
FROM
    postgres.company_type AS ct
    INNER JOIN postgres.movie_companies AS mc ON ct.id = mc.company_type_id
    INNER JOIN postgres.title AS t ON ct.id = t.id;
 