SELECT COUNT(*) FROM postgres.posts as p, postgres.tags as t, postgres.votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2010-07-20 02:01:05'::timestamp;
