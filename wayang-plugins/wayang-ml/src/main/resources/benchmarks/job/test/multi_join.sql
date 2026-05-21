SELECT 
    mc.note AS production_note
FROM 
    postgres.company_type AS ct
    INNER JOIN postgres.movie_companies AS mc ON ct.id = mc.company_type_id
                                              AND ct.kind = mc.note;