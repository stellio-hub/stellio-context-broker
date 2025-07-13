ALTER TABLE subscription
    ADD show_changes boolean;

UPDATE subscription
    SET show_changes = false;
