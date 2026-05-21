SELECT
    MIN(mi.info) AS release_date,
    MIN(t.title) AS modern_american_internet_movie
FROM
    postgres.aka_title AS at_alias,
    postgres.company_name AS cn,
    postgres.company_type AS ct,
    postgres.info_type AS it1,
    postgres.keyword AS k,
    postgres.movie_companies AS mc,
    postgres.movie_info AS mi,
    postgres.movie_keyword AS mk,
    postgres.title AS t
WHERE
    cn.country_code = '[us]' AND
    it1.info = 'release dates' AND
    mi.note LIKE '%internet%' AND
    mi.info IS NOT NULL AND
    (mi.info LIKE 'USA:% 199%' OR mi.info LIKE 'USA:% 200%') AND
    t.production_year > 1990 AND
    t.id = at_alias.movie_id AND
    t.id = mi.movie_id AND
    t.id = mk.movie_id AND
    t.id = mc.movie_id AND
    mk.movie_id = mi.movie_id AND
    mk.movie_id = mc.movie_id AND
    mk.movie_id = at_alias.movie_id AND
    mi.movie_id = mc.movie_id AND
    mi.movie_id = at_alias.movie_id AND
    mc.movie_id = at_alias.movie_id AND
    k.id = mk.keyword_id AND
    it1.id = mi.info_type_id AND
    cn.id = mc.company_id AND
    ct.id = mc.company_type_id AND
    cn.name_pcode_nf = cn.name_pcode_sf;
