create database "context_search";
create user datahub;
ALTER USER datahub WITH PASSWORD 'password';
grant all privileges on database context_search to datahub;
