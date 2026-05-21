SELECT
    MIN(mi.info) AS release_date,
    MIN(miidx.info) AS rating,
    MIN(t.title) AS german_movie
FROM
    postgres.company_name AS cn,
    postgres.company_type AS ct,
    postgres.info_type AS it,
    postgres.info_type AS it2,
    postgres.kind_type AS kt,
    postgres.movie_companies AS mc,
    postgres.movie_info AS mi,
    postgres.movie_info_idx AS miidx,
    postgres.title AS t
WHERE
    cn.country_code ='[de]' AND
    ct.kind ='production companies' AND
    it.info ='rating' AND
    it2.info ='release dates' AND
    kt.kind ='movie' AND
    mi.movie_id = t.id AND
    it2.id = mi.info_type_id AND
    kt.id = t.kind_id AND
    mc.movie_id = t.id AND
    cn.id = mc.company_id AND
    ct.id = mc.company_type_id AND
    miidx.movie_id = t.id AND
    it.id = miidx.info_type_id AND
    mi.movie_id = miidx.movie_id AND
    mi.movie_id = mc.movie_id AND
    miidx.movie_id = mc.movie_id AND
    cn.name_pcode_nf = cn.name_pcode_sf AND
    mi.movie_id = miidx.movie_id;
