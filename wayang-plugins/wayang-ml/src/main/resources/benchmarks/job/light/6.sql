SELECT COUNT(*) FROM postgres.title t, postgres.movie_info mi, postgres.movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>2005;
