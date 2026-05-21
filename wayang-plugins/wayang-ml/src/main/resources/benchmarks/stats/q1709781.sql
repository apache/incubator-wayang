SELECT COUNT(*) FROM postgres.comments as c, postgres.postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=1 AND ph.CreationDate>='2010-09-14 11:59:07'::timestamp;
