SELECT COUNT(*) FROM postgres.cast_info ci, postgres.title t, postgres.movie_companies mc WHERE t.id=ci.movie_id AND t.id=mc.movie_id AND ci.role_id=7;
