-- Create temp table for groups and import data

create table temp_info_groups (sub text, name text, subject_info jsonb);
copy temp_info_groups(sub, name) from '/tmp/export_groups.csv' csv header;
update temp_info_groups
    set subject_info = jsonb_build_object(
        'type', 'Property',
        'value', jsonb_build_object('kind', 'Group', 'name', name)
    );
update subject_referential
    set subject_info = (
        select subject_info 
        from temp_info_groups
        where sub = subject_referential.subject_id
    )
    where subject_type = 'GROUP';
update subject_referential
    set subject_info = jsonb_build_object(
        'type', 'Property',
        'value', jsonb_build_object('kind', 'Group')
    )
    where subject_type = 'GROUP'
    and subject_info is null;

-- Create temp table for users and import data

create table temp_info_users (sub text, username text, given_name text, family_name text, subject_info jsonb);
copy temp_info_users(sub, username, given_name, family_name) from '/tmp/export_users.csv' csv header;
update temp_info_users
    set given_name = null 
    where given_name = 'null';
update temp_info_users
    set family_name = null 
    where family_name = 'null';
update temp_info_users
    set subject_info = jsonb_build_object(
        'type', 'Property',
        'value',  jsonb_strip_nulls(
            jsonb_build_object('kind', 'User', 'username', username, 'givenName', given_name, 'familyName', family_name)
        )
    );
update subject_referential
    set subject_info = (
        select subject_info 
        from temp_info_users
        where sub = subject_referential.subject_id
    )
    where subject_type = 'USER';

update subject_referential
    set subject_info = jsonb_build_object(
        'type', 'Property',
        'value', jsonb_build_object('kind', 'User')
    )
    where subject_type = 'USER'
    and subject_info is null;

-- Create temp table for clients and import data

create table temp_info_clients (sub text, sid text, client_id text, subject_info jsonb);
copy temp_info_clients(sub, sid, client_id) from '/tmp/export_clients.csv' csv header;
update temp_info_clients
    set subject_info = jsonb_build_object(
        'type', 'Property',
        'value', jsonb_build_object('kind', 'Client', 'clientId', client_id)
    );
update subject_referential
    set subject_info = (
        select subject_info 
        from temp_info_clients
        where sub = subject_referential.subject_id
    )
    where subject_type = 'CLIENT';

update subject_referential
    set subject_info = jsonb_build_object(
        'type', 'Property',
        'value', jsonb_build_object('kind', 'Client')
    )
    where subject_type = 'CLIENT'
    and subject_info is null;

-- Delete temp tables

drop table temp_info_groups;
drop table temp_info_users;
drop table temp_info_clients;
