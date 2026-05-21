SELECT
    MIN(mi.info) AS movie_budget,
    MIN(mi_idx.info) AS movie_votes,
    MIN(n.name) AS writer,
    MIN(t.title) AS complete_gore_movie
FROM
    postgres.complete_cast AS cc,
    postgres.comp_cast_type AS cct1,
    postgres.comp_cast_type AS cct2,
    postgres.cast_info AS ci,
    postgres.info_type AS it1,
    postgres.info_type AS it2,
    postgres.keyword AS k,
    postgres.movie_info AS mi,
    postgres.movie_info_idx AS mi_idx,
    postgres.movie_keyword AS mk,
    postgres.name AS n,
    postgres.title AS t,
    postgres.char_name AS c
WHERE
    cct1.kind IN ('cast', 'crew') AND
    cct2.kind ='complete+verified' AND
    ci.note IN ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)') AND
    it1.info = 'genres' AND
    it2.info = 'votes' AND
    k.keyword IN ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital') AND
    mi.info IN ('Horror', 'Thriller') AND
    n.gender = 'm' AND
    t.production_year > 2000 AND
    (t.title LIKE '%Freddy%' OR t.title LIKE '%Jason%' OR t.title LIKE 'Saw%') AND
    t.id = mi.movie_id AND
    t.id = mi_idx.movie_id AND
    t.id = ci.movie_id AND
    t.id = mk.movie_id AND
    t.id = cc.movie_id AND
    ci.movie_id = mi.movie_id AND
    ci.movie_id = mi_idx.movie_id AND
    ci.movie_id = mk.movie_id AND
    ci.movie_id = cc.movie_id AND
    mi.movie_id = mi_idx.movie_id AND
    mi.movie_id = mk.movie_id AND
    mi.movie_id = cc.movie_id AND
    mi_idx.movie_id = mk.movie_id AND
    mi_idx.movie_id = cc.movie_id AND
    mk.movie_id = cc.movie_id AND
    n.id = ci.person_id AND
    it1.id = mi.info_type_id AND
    it2.id = mi_idx.info_type_id AND
    k.id = mk.keyword_id AND
    cct1.id = cc.subject_id AND
    cct2.id = cc.status_id AND
    c.name_pcode_nf = n.name_pcode_nf AND
    c.surname_pcode = n.surname_pcode;
