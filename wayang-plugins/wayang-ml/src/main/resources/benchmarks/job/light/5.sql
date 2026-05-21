SELECT COUNT(*) FROM postgres.movie_companies mc, postgres.title t, postgres.movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id=117;
