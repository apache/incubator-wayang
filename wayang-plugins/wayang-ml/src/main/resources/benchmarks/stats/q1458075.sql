SELECT COUNT(*) FROM postgres.comments as c, postgres.postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=0 AND ph.PostHistoryTypeId=1;
