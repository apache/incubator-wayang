SELECT COUNT(*) FROM postgres.votes as v, postgres.badges as b, postgres.users as u WHERE u.Id = v.UserId AND v.UserId = b.UserId AND u.DownVotes>=0 AND u.DownVotes<=0;
