SELECT COUNT(*) FROM postgres.comments as c, postgres.votes as v WHERE c.UserId = v.UserId AND c.Score=0;
