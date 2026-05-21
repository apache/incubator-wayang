SELECT
    MIN(cn.name) AS company_name,
    MIN(lt.link) AS link_type,
    MIN(t.title) AS western_follow_up
FROM
    postgres.company_name AS cn,
    postgres.company_type AS ct,
    postgres.keyword AS k,
    postgres.link_type AS lt,
    postgres.movie_companies AS mc,
    postgres.movie_info AS mi,
    postgres.movie_keyword AS mk,
    postgres.movie_link AS ml,
    postgres.title AS t
WHERE
    cn.country_code <> '[pl]' AND
    (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%') AND
    ct.kind ='production companies' AND
    k.keyword ='sequel' AND
    lt.link LIKE '%follow%' AND
    mc.note IS NULL AND
    mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German') AND
    t.production_year BETWEEN 1950 AND 2000 AND
    lt.id = ml.link_type_id AND
    ml.movie_id = t.id AND
    t.id = mk.movie_id AND
    mk.keyword_id = k.id AND
    t.id = mc.movie_id AND
    mc.company_type_id = ct.id AND
    mc.company_id = cn.id AND
    mi.movie_id = t.id AND
    ml.movie_id = mk.movie_id AND
    ml.movie_id = mc.movie_id AND
    mk.movie_id = mc.movie_id AND
    ml.movie_id = mi.movie_id AND
    mk.movie_id = mi.movie_id AND
    mc.movie_id = mi.movie_id AND
    cn.name_pcode_nf = cn.name_pcode_sf;
