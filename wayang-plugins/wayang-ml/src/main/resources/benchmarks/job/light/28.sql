SELECT COUNT(*) FROM postgres.title t, postgres.cast_info ci, postgres.movie_companies mc WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>2005 AND ci.role_id=1;
