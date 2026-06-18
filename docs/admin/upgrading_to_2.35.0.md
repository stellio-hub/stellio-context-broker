# Upgrading to 2.35.0

This note describes the necessary steps to upgrade to Stellio 2.35.0

## Migration Issue for compressed temporal data setup

> ** WARNING ** This only concerns setup that [enabled timescale compression](enabling_timescale_compression.md).


Timescale does not support data type changes [on compressed data](https://github.com/timescale/docs/blob/latest/use-timescale/compression/modify-a-schema.md#schema-modifications)
It means that to migrate to 2.35.0, you should first decompress 'attribute_instance' and 'attribute_instance_audit' tables. (which may take a lot of space)

````sql
WITH chunks AS (SELECT format('%I.%I', chunk_schema, chunk_name)::regclass AS chunk
                FROM timescaledb_information.chunks
                WHERE hypertable_name IN ( '{tenant_db}.attribute_instance','{tenant_db}.attribute_instance_audit')
                  AND is_compressed = true)

SELECT decompress_chunk(chunk)
FROM chunks;
````

You can check that all the chunks have been correctly decompressed by running:

```sql
SELECT number_compressed_chunks
FROM hypertable_compression_stats('{tenant_db}.attribute_instance');
```