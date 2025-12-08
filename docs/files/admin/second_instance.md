# Launch a second instance of Stellio

The `.context-source.env` file contains the necessary configurations required to run a second instance of Stellio.
You can use it with:
```shell
docker compose --env-file .env --env-file .context-source.env -p stellio-context-source up
```

This will launch the instance using the following ports:
- api-gateway: 8090
- search-service: 8093
- subscription-service: 8094
- postgres: 5433
- kafka: 29093

- You can change them by editing the variables in the `.context-source.env` file.