update subscription
set modified_at = created_at
where modified_at is null;

alter table subscription
    alter column modified_at set not null;

