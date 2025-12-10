# Temporal data compression

**Warning: it only works with TimescaleDB 2.18 or above (or stellio/stellio-timescale-postgis:16-2.20.2-3.5 or above, which will be default version starting from Stellio 2.24.0)**

## Enable compression

You can enable TimescaleDB compression on temporal instances by running:

```sql
-- enable column store
ALTER TABLE attribute_instance SET(
    timescaledb.enable_columnstore,
    timescaledb.orderby = 'time DESC',
    timescaledb.segmentby = 'temporal_entity_attribute'
);

-- add compression policy
CALL add_columnstore_policy('attribute_instance', after => INTERVAL '30d');

-- prevent potential "tuple decompression limit exceeded by operation" issue
SET timescaledb.max_tuples_decompressed_per_dml_transaction = 1000000;
```
The `after => INTERVAL '30d'` setting specifies that a chunk will be compressed after 30 days.
See [TimescaleDB documentation](https://docs.tigerdata.com/api/latest/hypertable/) for more information on setting up the compression.

## Configure compression

### Configure the job

Enabling compression creates a job that compresses data every 12h. You can find the job by using:

```sql
-- get all compression jobs on attribute_instance
SELECT * FROM timescaledb_information.jobs
WHERE proc_name = 'policy_compression'
AND hypertable_name = 'attribute_instance';
```

You can also change the scheduled interval with:

```sql
-- change execution to once a day
-- replace 1000 by your job id if different
SELECT alter_job(1000, schedule_interval=>'24 hours'::interval);
```

### Configure the chunk

TimescaleDB compression works with chunks, a chunk is a part of the table that is compressed.
By default a chunk contains 7 days of data, which is optimal for data points every hour to every 10 minutes. 
Increasing the chunk time interval will reduce the size of your data but will impact request performance (see [choosing the right chunk interval](https://docs.timescale.com/use-timescale/latest/hypertables/#best-practices-for-time-partitioning)).

```sql
SELECT set_chunk_time_interval('attribute_instance', INTERVAL '30 days');
```

### Troubleshooting

If you run into "tuple decompression limit exceeded by operation" error you should augment the `max_tuples_decompressed_per_dml_transaction variables`:

```sql
SET timescaledb.max_tuples_decompressed_per_dml_transaction = 1000000;
```

You can find other ideas for troubleshooting [here](https://docs.timescale.com/use-timescale/latest/compression/troubleshooting/).

## Compression statistics

You can see the statistics of your compression with the following command:

```sql
SELECT * FROM hypertable_compression_stats('attribute_instance');
```

## Disable compression

You can disable compression rule by calling:

```sql
CALL remove_columnstore_policy('attribute_instance');
```

## Manipulate chunks

See your chunks:

```sql
-- with chunk names
SELECT show_chunks('attribute_instance');

-- with some statistics
SELECT * FROM chunk_columnstore_stats('attribute_instance');

-- with even more statistics
SELECT  *
FROM timescaledb_information.chunks
WHERE hypertable_name = 'attribute_instance';
```

Compress or decompress a chunk:

```sql
SELECT compress_chunk('_timescaledb_internal._hyper_2_1_chunk');
SELECT decompress_chunk('_timescaledb_internal._hyper_2_1_chunk');
```

Split or merge chunks:

```sql
CALL split_chunk('_timescaledb_internal._hyper_2_8_chunk','2025-06-15 00:00:00.000000 +00:00');
CALL merge_chunks('_timescaledb_internal._hyper_2_1_chunk', '_timescaledb_internal._hyper_2_2_chunk');
```

## Performance considerations

### Data storage

Enabling TimescaleDB compression can help reduce the size taken by the temporal instances by up to 80% for huge chunk size.

### Endpoint response time 

#### Basic consumption (GET /entities)

The basic consumptions endpoints are not impacted by the temporal data compression.

#### Temporal Consumption (GET /temporal)

Compression has a really low impact on temporal data consumption.
We have noticed a 12-20 ms increased time for temporal consumption requests when enabling temporal data compression.

Example of request impact :
 - get 100 entities with lastN=1 :  130ms → 147ms (13%)
 - get 200 entities with lastN=1 :  201ms → 214ms (6%)
 - get 100 entities with all data:  1030ms → 1051ms (2%)

#### Provision of recent data

Data more recent than the interval specified in add_columnstore_policy is not compressed.
This means that compression has no impact on inserting new values or modifying recent data.

####  Provision and modification of old data

If you insert or modify data within a compressed chunk (i.e., older data), you may experience performance impacts on your requests.
The impact depends directly on the size of the chunk being modified.

Example of request impact on a large chunk:
 - Updating a temporal instance: 23 ms → 207 ms
