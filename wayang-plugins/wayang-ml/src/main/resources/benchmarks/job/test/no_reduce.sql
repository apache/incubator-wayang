SELECT 
    mc.note AS production_note, 
    t.title AS movie_title, 
    t.production_year AS movie_year
FROM postgres.company_type ct
JOIN postgres.movie_companies mc ON ct.id = mc.company_type_id
JOIN postgres.title t ON t.id = mc.movie_id
WHERE 
    ct.kind = 'production companies' 
    AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%' 
    AND (mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%');
