SELECT COUNT(*) FROM postgres.postHistory as ph, postgres.votes as v, postgres.users as u, postgres.badges as b WHERE u.Id = b.UserId AND u.Id = ph.UserId AND u.Id = v.UserId AND u.Views>=0;
