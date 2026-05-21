SELECT COUNT(*) FROM postgres.badges as b, postgres.users as u WHERE b.UserId= u.Id AND u.UpVotes>=0;
