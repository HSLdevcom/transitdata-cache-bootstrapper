[![Build Status](https://travis-ci.org/HSLdevcom/transitdata-cache-bootstrapper.svg?branch=master)](https://travis-ci.org/HSLdevcom/transitdata-cache-bootstrapper)

# Transitdata-Cache-Bootstrapper

This project is part of the [Transitdata Pulsar-pipeline](https://github.com/HSLdevcom/transitdata).

## Description

This application fetches information about DatedVehicleJourneys from Pubtrans and writes it to Redis.
The Redis cache is then used to match DatedVehicleJourney Ids to route name, direction and trip start time
in the following steps of the pipeline.
