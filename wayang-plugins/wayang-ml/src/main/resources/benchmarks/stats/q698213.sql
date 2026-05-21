SELECT COUNT(*) FROM postgres.comments as c, postgres.posts as p, postgres.postHistory as ph WHERE p.Id = c.PostId AND p.Id = ph.PostId AND p.CommentCount>=0 AND p.CommentCount<=25;
