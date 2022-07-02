drop table if exists public.outbox; 

create table if not exists public.outbox (
    id integer primary key generated always as identity,
    object_id integer not null,
    record_ts timestamp not null,
    type varchar not null,
    payload text not null
);

-- Двигайтесь дальше! Ваш код: nUzKgiynco
