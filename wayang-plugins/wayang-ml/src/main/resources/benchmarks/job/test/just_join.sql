SELECT 
    mc.note AS production_note,
    t.title AS movie_title,
    t.production_year AS movie_year
FROM 
    postgres.company_type AS ct
    INNER JOIN postgres.movie_companies AS mc ON ct.id = mc.company_type_id
    INNER JOIN postgres.title AS t ON t.id = mc.movie_id
    INNER JOIN postgres.movie_info_idx AS mi_idx ON t.id = mi_idx.movie_id 
    INNER JOIN postgres.info_type AS it ON it.id = mi_idx.info_type_id;