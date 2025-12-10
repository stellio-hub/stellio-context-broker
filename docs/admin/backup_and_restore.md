# Backup a Stellio instance

Below is a basic script that can be used to backup the Stellio databases. Feel free to take it as a basis (don't forget to review the environement variables at the top of the script) and adapt it to fit your needs.

```shell
#!/usr/bin/env bash

# Number of days of history kept for backups
# 1st argument passed on the command line, 30 either
DAYS_HISTORY=${1:-30}
# Where the backups will be put
BACKUP_DIR=$HOME/backup/stellio
# Where docker-compose file for Stellio is installed
STELLIO_COMPOSE_DIR=$HOME/stellio-launcher

source $STELLIO_COMPOSE_DIR/.env

mkdir -p $BACKUP_DIR
now=$(date +%Y-%m-%d)
echo "Backups will be suffixed with date $now and placed into $HOME/backup/stellio"

echo
echo "Performing a backup of PostgreSQL databases (search and subscription)"

# Following instructions from https://docs.timescale.com/latest/using-timescaledb/backup#pg_dump-pg_restore
# It display warnings ... that can be ignored
# Cf https://github.com/timescale/timescaledb/issues/1581
docker exec postgres /bin/bash -c "su - postgres -c 'pg_dump -Fc stellio_search | gzip -c'" > /tmp/postgres_search_$now.gz
docker exec postgres /bin/bash -c "su - postgres -c 'pg_dump -Fc stellio_subscription | gzip -c'" > /tmp/postgres_subscription_$now.gz

mv /tmp/postgres_search_$now.gz $BACKUP_DIR/.
mv /tmp/postgres_subscription_$now.gz $BACKUP_DIR/.

echo
echo "Deleting backups older than $DAYS_HISTORY days"

find $BACKUP_DIR -mtime +$DAYS_HISTORY -delete

echo
echo "Backup terminated!"
```

You can call it in a cron job like this:

```shell
30 1 * * * /path/to/backup_databases.sh # called every night at 1:30am
```

# Restore a Stellio instance

## Set some environment variables

WARNING : backup date has to be set up manually in other places below, review them carefully!

```shell
backup_date=2023-01-27
export BACKUP_DIR=$HOME/backup/stellio
export STELLIO_COMPOSE_DIR=$HOME/stellio-launcher
```

## Step 1 - Restore PG

* Launch Postgres container and connect into it

```shell
cd $STELLIO_COMPOSE_DIR
docker compose up -d postgres

docker cp $BACKUP_DIR/postgres_search_$backup_date.gz stellio-postgres:/tmp/.
docker cp $BACKUP_DIR/postgres_subscription_$backup_date.gz stellio-postgres:/tmp/.

docker exec -it stellio-postgres bash
```

* Once in the container, restore the databases

```shell
backup_date=2023-01-27 # need to be set again in the container
gunzip /tmp/postgres_search_$backup_date.gz
gunzip /tmp/postgres_subscription_$backup_date.gz

# then following https://docs.timescale.com/timescaledb/latest/how-to-guides/backup-and-restore/pg-dump-and-restore/#entire-database
su - postgres
psql

\c stellio_search
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT timescaledb_pre_restore();
\! pg_restore -Fc -d stellio_search /tmp/postgres_search_2023-01-27 -- change the date!
SELECT timescaledb_post_restore();

\c stellio_subscription
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT timescaledb_pre_restore();
\! pg_restore -Fc -d stellio_subscription /tmp/postgres_subscription_2023-01-27-- change the date!
SELECT timescaledb_post_restore();

exit # from psql
exit # from postgres user and be root again

rm -f /tmp/postgres_search_*
rm -f /tmp/postgres_subscription_*

exit # from the container
```

* Stop the Postgres container

```shell
docker compose stop postgres
```

## Step 3 - Restart Stellio

```shell
cd $STELLIO_COMPOSE_DIR
docker compose up -d && docker compose logs -f --tail=100
```
