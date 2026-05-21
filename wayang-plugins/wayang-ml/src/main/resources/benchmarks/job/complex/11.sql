SELECT
    MIN(chn.name) AS character_name,
    MIN(mi_idx.info) AS rating,
    MIN(t.title) AS complete_hero_movie
FROM
    postgres.complete_cast AS cc,
    postgres.comp_cast_type AS cct1,
    postgres.comp_cast_type AS cct2,
    postgres.char_name AS chn,
    postgres.cast_info AS ci,
    postgres.info_type AS it2,
    postgres.keyword AS k,
    postgres.kind_type AS kt,
    postgres.movie_info_idx AS mi_idx,
    postgres.movie_keyword AS mk,
    postgres.name AS n,
    postgres.title AS t,
    postgres.aka_name AS akn,
    postgres.aka_title AS akt
WHERE
    cct1.kind = 'cast' AND
    cct2.kind LIKE '%complete%' AND
    chn.name IS NOT NULL AND
    (chn.name LIKE '%man%' OR chn.name LIKE '%Man%') AND
    it2.info = 'rating' AND
    k.keyword IN ('superhero', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence', 'magnet', 'web', 'claw', 'laser') AND
    kt.kind = 'movie' AND
    t.production_year > 2000 AND
    kt.id = t.kind_id AND
    t.id = mk.movie_id AND
    t.id = ci.movie_id AND
    t.id = cc.movie_id AND
    t.id = mi_idx.movie_id AND
    mk.movie_id = ci.movie_id AND
    mk.movie_id = cc.movie_id AND
    mk.movie_id = mi_idx.movie_id AND
    ci.movie_id = cc.movie_id AND
    ci.movie_id = mi_idx.movie_id AND
    cc.movie_id = mi_idx.movie_id AND
    chn.id = ci.person_role_id AND
    n.id = ci.person_id AND
    k.id = mk.keyword_id AND
    cct1.id = cc.subject_id AND
    cct2.id = cc.status_id AND
    it2.id = mi_idx.info_type_id AND
    akn.imdb_index = akt.imdb_index AND
    ci.nr_order = cc.status_id AND
    akt.imdb_index = t.imdb_index;
