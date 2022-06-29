select 
    table_name,
    column_name,
    data_type,
    character_maximum_length,
    column_default,
    is_nullable
from information_schema.columns
where table_schema = 'public';
