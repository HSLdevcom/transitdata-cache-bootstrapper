[![Build Status](https://travis-ci.org/HSLdevcom/transitdata-cache-bootstrapper.svg?branch=master)](https://travis-ci.org/HSLdevcom/transitdata-cache-bootstrapper)

# Transitdata-Cache-Bootstrapper

This project is part of the [Transitdata Pulsar-pipeline](https://github.com/HSLdevcom/transitdata).

## Description

This application fetches information about DatedVehicleJourneys from Pubtrans and writes it to Redis.
The Redis cache is then used to match DatedVehicleJourney Ids to route name, direction and trip start time
in the following steps of the pipeline.

Application also stores the timestamp of the latest update in ISO-8601 format (f.ex 2018-12-24T07:07:07.007Z) 
to Redis after each successful update.
