update subscription
set modified_at = created_at
where modified_at is null;
