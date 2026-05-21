SELECT
    MIN(n.name) AS member_in_charnamed_american_movie,
    MIN(n.name) AS a1
FROM
    postgres.cast_info AS ci,
    postgres.company_name AS cn,
    postgres.keyword AS k,
    postgres.movie_companies AS mc,
    postgres.movie_keyword AS mk,
    postgres.name AS n,
    postgres.title AS t
WHERE
    cn.country_code ='[us]' AND
    k.keyword ='character-name-in-title' AND
    n.name LIKE 'B%' AND
    n.id = ci.person_id AND
    ci.movie_id = t.id AND
    t.id = mk.movie_id AND
    mk.keyword_id = k.id AND
    t.id = mc.movie_id AND
    mc.company_id = cn.id AND
    ci.movie_id = mc.movie_id AND
    ci.movie_id = mk.movie_id AND
    mc.movie_id = mk.movie_id AND
    n.name_pcode_nf = cn.name_pcode_nf AND
    n.name_pcode_nf = cn.name_pcode_sf;
