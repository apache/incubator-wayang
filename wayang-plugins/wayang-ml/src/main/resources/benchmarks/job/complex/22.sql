SELECT
    MIN(k.keyword) AS movie_keyword,
    MIN(n.name) AS actor_name,
    MIN(t.title) AS hero_movie
FROM
    postgres.cast_info AS ci,
    postgres.keyword AS k,
    postgres.movie_keyword AS mk,
    postgres.name AS n,
    postgres.title AS t,
    postgres.char_name AS cn
WHERE
    k.keyword IN ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence') AND
    n.name LIKE '%Downey%Robert%' AND
    t.production_year > 2000 AND
    k.id = mk.keyword_id AND
    t.id = mk.movie_id AND
    t.id = ci.movie_id AND
    ci.movie_id = mk.movie_id AND
    n.id = ci.person_id AND
    cn.name_pcode_nf = n.name_pcode_nf AND
    cn.surname_pcode = n.surname_pcode;
