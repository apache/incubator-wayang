SELECT COUNT(*) FROM postgres.cast_info ci, postgres.title t WHERE t.id=ci.movie_id AND t.production_year>1980 AND t.production_year<1995;
