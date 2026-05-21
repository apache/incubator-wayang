SELECT COUNT(*) FROM postgres.movie_keyword mk, postgres.title t, postgres.cast_info ci WHERE t.id=mk.movie_id AND t.id=ci.movie_id AND t.production_year>2014;
