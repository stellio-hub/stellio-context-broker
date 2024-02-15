ALTER TABLE subscription
    ADD sys_attrs boolean;

UPDATE subscription
    SET sys_attrs = false;
