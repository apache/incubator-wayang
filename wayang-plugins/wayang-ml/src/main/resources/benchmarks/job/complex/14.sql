SELECT
    MIN(k.keyword) AS movie_keyword,
    MIN(n.name) AS actor_name,
    MIN(t.title) AS marvel_movie
FROM
    postgres.cast_info AS ci,
    postgres.keyword AS k,
    postgres.movie_keyword AS mk,
    postgres.name AS n,
    postgres.title AS t,
    postgres.aka_name AS ak,
    postgres.company_name AS cn
WHERE
    k.keyword = 'marvel-cinematic-universe' AND
    n.name LIKE '%Downey%' AND
    t.production_year > 2000 AND
    k.id = mk.keyword_id AND
    t.id = mk.movie_id AND
    t.id = ci.movie_id AND
    ci.movie_id = mk.movie_id AND
    n.id = ci.person_id AND
    ak.name_pcode_cf = n.name_pcode_cf AND
    ak.name_pcode_nf = cn.name_pcode_nf AND
    ak.name_pcode_nf = cn.name_pcode_sf AND
    ak.surname_pcode = n.surname_pcode;
