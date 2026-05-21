SELECT
    MIN(mc.note) AS production_note,
    MIN(t.title) AS movie_title,
    MIN(t.production_year) AS movie_year
FROM
    postgres.company_type ct,
    postgres.movie_companies mc,
    postgres.title t
WHERE
    ct.id = mc.company_type_id
    AND t.id = mc.movie_id
    AND ct.kind = 'production companies'
    AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
    AND (mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%');
