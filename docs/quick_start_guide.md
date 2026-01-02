# Overview

# Quick start

A quick way to start using Stellio is to use the provided `docker-compose.yml` file in the root directory 
(feel free to change the default passwords defined in the `.env` file):

```shell
docker compose up -d && docker compose logs -f
```

It will start all the services composing the Stellio context broker platform and expose them on the following ports:

-   API Gateway: 8080 (see [architecture](admin/architecture.md) for more details)
-   Search service: 8083
-   Subscription service: 8084

Docker images are available on [Docker Hub](https://hub.docker.com/orgs/stellio/repositories).)


# Usage

To start using Stellio, you can follow the [API quick guide](user/API_walkthrough.md).

As the development environment does not make use of the authentication setup, you can ignore related information in the API quick guide.
