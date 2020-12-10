
create table public.device
(
    device_id text constraint device_pk primary key,
    last_seen timestamp default current_timestamp
);

create or replace function device_log() returns trigger as
$BODY$
begin
    insert into device(device_id, last_seen)
    values(new.device_id, current_timestamp)
    on conflict (device_id)
    do update set last_seen = current_timestamp;
        return new;
end;
$BODY$
language plpgsql;

create trigger device_log_trigger
    after insert on message
    for each row
    execute procedure device_log();
