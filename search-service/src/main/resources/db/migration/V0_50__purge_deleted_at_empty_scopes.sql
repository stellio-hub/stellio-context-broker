delete from scope_history
    where time_property = 'DELETED_AT'
    and value = '[]'::jsonb;
