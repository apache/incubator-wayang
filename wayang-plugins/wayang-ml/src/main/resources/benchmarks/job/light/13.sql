SELECT COUNT(*) FROM postgres.title t, postgres.movie_info mi, postgres.movie_companies mc WHERE t.id=mi.movie_id AND t.id=mc.movie_id AND t.production_year>2010 AND mc.company_type_id=2;
