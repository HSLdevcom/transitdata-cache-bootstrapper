# Transitdata-Cache-Bootstrapper [![Test and create Docker image](https://github.com/HSLdevcom/transitdata-cache-bootstrapper/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/transitdata-cache-bootstrapper/actions/workflows/test-and-build.yml)

This project is part of the [Transitdata](https://github.com/HSLdevcom/transitdata).

## Description

This application fetches information about DatedVehicleJourneys from PubTrans and writes it to Redis.
The Redis cache is then used to match DatedVehicleJourney Ids to route name, direction and trip start time
in the following steps of the pipeline.

Application also stores the timestamp of the latest update in ISO-8601 format (f.ex `2018-12-24T07:07:07.007Z`) 
to Redis after each successful update. This timestamp is used by other Transitdata applications to make sure that they are not using outdated data.

This application is *not* connected to Pulsar.

## Running

### Dependencies

* Redis

### Environment variables

* `REDIS_HOST`: Redis hostname
* `REDIS_PORT`: port to use to connect to Redis
* `REDIS_TTL_DAYS`: for how long the date is valid
* `QUERY_HISTORY_DAYS`: how many days to past to query from PubTrans DOI
* `QUERY_FUTURE_DAYS`: how many days to future to query from PubTrans DOI

Don't use too long timespan with variables `QUERY_HISTORY_DAYS` and `QUERY_FUTURE_DAYS` to avoid problems with performance.
