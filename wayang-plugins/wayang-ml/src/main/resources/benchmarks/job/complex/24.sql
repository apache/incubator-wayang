SELECT
    MIN(n.name) AS member_in_charnamed_movie
FROM
    postgres.cast_info AS ci,
    postgres.company_name AS cn,
    postgres.keyword AS k,
    postgres.movie_companies AS mc,
    postgres.movie_keyword AS mk,
    postgres.name AS n,
    postgres.title AS t,
    postgres.char_name AS cn2,
    postgres.aka_name AS ak
WHERE
    k.keyword ='character-name-in-title' AND
    n.name LIKE '%Bert%' AND
    n.id = ci.person_id AND
    ci.movie_id = t.id AND
    t.id = mk.movie_id AND
    mk.keyword_id = k.id AND
    t.id = mc.movie_id AND
    mc.company_id = cn.id AND
    ci.movie_id = mc.movie_id AND
    ci.movie_id = mk.movie_id AND
    mc.movie_id = mk.movie_id AND
    ak.name_pcode_cf = cn2.name_pcode_nf AND
    ak.name_pcode_nf = n.name_pcode_cf AND
    ak.name_pcode_nf = cn2.surname_pcode AND
    ak.name_pcode_cf = cn.name_pcode_sf;
