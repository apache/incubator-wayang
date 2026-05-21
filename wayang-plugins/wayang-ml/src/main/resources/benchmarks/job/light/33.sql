SELECT COUNT(*) FROM postgres.title t, postgres.movie_keyword mk, postgres.movie_companies mc WHERE t.id=mk.movie_id AND t.id=mc.movie_id AND t.production_year>1950;
