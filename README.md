# Transitdata-Cache-Bootstrapper

This tool fetches information about DatedVehicleJourneys from Pubtrans and writes it to Redis.
The Redis cache is then used to match DatedVehicleJourney Ids to route name, direction and trip start time
in the following steps of the pipeline.
