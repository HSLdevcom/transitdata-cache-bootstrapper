package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.redis.RedisStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import static fi.hsl.common.transitdata.TransitdataProperties.KEY_DIRECTION;
import static fi.hsl.common.transitdata.TransitdataProperties.KEY_OPERATING_DAY;
import static fi.hsl.common.transitdata.TransitdataProperties.KEY_ROUTE_NAME;
import static fi.hsl.common.transitdata.TransitdataProperties.KEY_START_TIME;
import static fi.hsl.common.transitdata.TransitdataProperties.REDIS_PREFIX_DVJ;
import static fi.hsl.common.transitdata.TransitdataProperties.formatJoreId;
import static fi.hsl.transitdata.pubtransredisconnect.JourneyResultSetProcessor.JourneyResultItem;

public class JourneyResultSetProcessor extends AbstractResultSetProcessor<JourneyResultItem> {

    private static final Logger log = LoggerFactory.getLogger(JourneyResultSetProcessor.class);
    private static final int BATCH_SIZE = 5000;

    record JourneyResultItem(String dvjId, String routeName, String direction, String operatingDay, String startTime) {
    }

    public JourneyResultSetProcessor(final RedisStore redisStore, QueryUtils queryUtils, Duration redisTtl) {
        super(redisStore, queryUtils, redisTtl);
    }

    @Override
    String getQuery() {
        return "SELECT " +
                "   DISTINCT CONVERT(CHAR(16), DVJ.Id) AS " + queryUtils.DVJ_ID + ", " +
                "   KVV.StringValue AS " + queryUtils.ROUTE_NAME + ", " +
                "   SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1) AS "
                + queryUtils.DIRECTION + ", " +
                "   CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS " + queryUtils.OPERATING_DAY + ", " +
                "   RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2) " +
                "       + ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime)) " +
                "       - ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS "
                + queryUtils.START_TIME + " " +
                "FROM ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ " +
                "LEFT JOIN ptDOI4_Community.dbo.VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id) " +
                "LEFT JOIN ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id) " +
                "LEFT JOIN ptDOI4_Community.T.KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id) " +
                "LEFT JOIN ptDOI4_Community.dbo.KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId) " +
                "LEFT JOIN ptDOI4_Community.dbo.KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId) " +
                "LEFT JOIN ptDOI4_Community.dbo.ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number) " +
                "WHERE " + "   ( " + "       KT.Name = 'JoreIdentity' " +
                "       OR KT.Name = 'JoreRouteIdentity' " + "       OR KT.Name = 'RouteName' " +
                "   ) " + "   AND OT.Name = 'VehicleJourney' " +
                "   AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL " +
                "   AND DVJ.OperatingDayDate >= '" + queryUtils.from + "' " +
                "   AND DVJ.OperatingDayDate < '" + queryUtils.to + "' " +
                "   AND DVJ.IsReplacedById IS NULL ";
    }

    @Override
    Collection<JourneyResultItem> collectResults(ResultSet resultSet) throws Exception {
        var items = new ArrayList<JourneyResultItem>();

        while (resultSet.next()) {
            items.add(new JourneyResultItem(
                    resultSet.getString(queryUtils.DVJ_ID),
                    resultSet.getString(queryUtils.ROUTE_NAME),
                    resultSet.getString(queryUtils.DIRECTION),
                    resultSet.getString(queryUtils.OPERATING_DAY),
                    resultSet.getString(queryUtils.START_TIME)));
        }

        return items;
    }

    @Override
    void processItems(Collection<JourneyResultItem> items) throws Exception {
        int tripInfoCounter = 0;
        int lookupCounter = 0;
        int rowCounter = 0;

        long timer = System.currentTimeMillis();
        long startTime = timer;
        for (var item : items) {
            rowCounter++;
            if (rowCounter % BATCH_SIZE == 0) {
                long elapsedBatch = System.currentTimeMillis() - timer;
                long secondsBatch = elapsedBatch / 1000;
                long minutesBatch = secondsBatch / 60;
                long remainingSecondsBatch = secondsBatch % 60;

                long elapsedTotal = System.currentTimeMillis() - startTime;
                long secondsTotal = elapsedTotal / 1000;
                long minutesTotal = secondsTotal / 60;
                long remainingSecondsTotal = secondsTotal % 60;

                log.info("Processed {} rows of {} in {} min {} sec. Took {} min {} sec to process {} rows.",
                        rowCounter, items.size(), minutesTotal, remainingSecondsTotal,
                        minutesBatch, remainingSecondsBatch, BATCH_SIZE);
                timer = System.currentTimeMillis();
            }

            final var values = new HashMap<String, String>();
            values.put(KEY_ROUTE_NAME, item.routeName);
            values.put(KEY_DIRECTION, item.direction);
            values.put(KEY_START_TIME, item.startTime);
            values.put(KEY_OPERATING_DAY, item.operatingDay);

            final var key = REDIS_PREFIX_DVJ + item.dvjId;
            var response = redisStore.setValues(key, values);

            if (redisStore.checkResponse(response)) {
                redisStore.setExpire(key, redisTtl);
                tripInfoCounter++;

                //Insert a composite key that allows reverse lookup of the dvj id
                //The format is route-direction-date-time
                final var joreKey = formatJoreId(
                        item.routeName, item.direction,
                        item.operatingDay, item.startTime);
                response = redisStore.setValue(joreKey, item.dvjId);
                if (redisStore.checkResponse(response)) {
                    redisStore.setExpire(joreKey, redisTtl);
                    lookupCounter++;
                } else {
                    log.error("Failed to set reverse-lookup key {}, Redis returned {}", joreKey, response);
                }
            } else {
                log.error("Failed to set Trip details for key {}, Redis returned {}", key, response);
            }
        }

        log.info("Inserted {} trip info and {} reverse-lookup keys for {} DB rows", tripInfoCounter, lookupCounter,
                rowCounter);
    }
}
