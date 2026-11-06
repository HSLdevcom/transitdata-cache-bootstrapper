# transitdata-cache-bootstrapper [![CI/CD](https://github.com/HSLdevcom/transitdata-cache-bootstrapper/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/HSLdevcom/transitdata-cache-bootstrapper/actions/workflows/ci-cd.yml)

This project is part of the [Transitdata Pulsar-pipeline](https://github.com/HSLdevcom/transitdata).

## Description

This application fetches information about DatedVehicleJourneys from Pubtrans and writes it to Redis.
The Redis cache is then used to match DatedVehicleJourney Ids to route name, direction and trip start time
in the following steps of the pipeline.

Application also stores the timestamp of the latest update in ISO-8601 format (f.ex 2018-12-24T07:07:07.007Z) 
to Redis after each successful update.

It runs as a one-shot job (an AKS CronJob): it queries Pubtrans once, writes the cache and exits with status 0,
or with status 1 if the job fails (e.g. the database can't be reached).

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

Requires Java 25. `transitdata-common` is resolved from GitHub Packages, so `GITHUB_ACTOR` and `GITHUB_TOKEN`
(a token with `read:packages`) must be set, or configured in `~/.m2/settings.xml`.

### Locally

- `./mvnw compile`
- `./mvnw test` runs the unit tests
- `./mvnw verify` also runs the integration tests (`*IT`, needs Docker for Testcontainers: SQL Server and
  Redis with Sentinel)
- `./mvnw spotless:apply` formats the code; CI only runs `spotless:check`
- `./mvnw package` builds `target/transitdata-cache-bootstrapper.jar`

### Docker image

- Run [this script](build-image.sh) to build the Docker image (passes `GITHUB_TOKEN` as a build secret)

## Running

Configuration is read from [environment.conf](src/main/resources/environment.conf), overridable with environment
variables:

| Variable | Description | Default |
|---|---|---|
| `TRANSITDATA_PUBTRANS_CONN_STRING` | JDBC connection string of the Pubtrans SQL Server | - |
| `REDIS_CLUSTER_SENTINELS` | Comma-separated `host:port` list of the Redis Sentinels | - |
| `REDIS_CLUSTER_MASTER_NAME` | Name of the Redis master monitored by the Sentinels | `mymaster` |
| `REDIS_TTL_DAYS` | Expiry of the journey keys written to Redis | `3` |
| `QUERY_HISTORY_DAYS` | Fetch journeys from this many days in the past | `3` |
| `QUERY_FUTURE_DAYS` | Fetch journeys up to this many days in the future | `90` |
| `HEALTH_ENABLED` | Start the health endpoint on port 8090 | `false` |
