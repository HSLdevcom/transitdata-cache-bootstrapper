package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class JourneyResultSetProcessor extends AbstractResultSetProcessor {

    private static final Logger log = LoggerFactory.getLogger(JourneyResultSetProcessor.class);

    private String from;
    private String to;

    public JourneyResultSetProcessor(final RedisUtils redisUtils, QueryUtils queryUtils) {
        super(redisUtils, queryUtils);
    }

    public void processResultSet(final ResultSet resultSet) throws Exception {
        int tripInfoCounter = 0;
        int lookupCounter = 0;
        int rowCounter = 0;

        while(resultSet.next()) {
            rowCounter++;
            final Map<String, String> values = new HashMap<>();
            values.put(TransitdataProperties.KEY_ROUTE_NAME, resultSet.getString(queryUtils.ROUTE_NAME));
            values.put(TransitdataProperties.KEY_DIRECTION, resultSet.getString(queryUtils.DIRECTION));
            values.put(TransitdataProperties.KEY_START_TIME, resultSet.getString(queryUtils.START_TIME));
            values.put(TransitdataProperties.KEY_OPERATING_DAY, resultSet.getString(queryUtils.OPERATING_DAY));

            final String key = TransitdataProperties.REDIS_PREFIX_DVJ + resultSet.getString(queryUtils.DVJ_ID);
            String response = redisUtils.setValues(key, values);

            if (redisUtils.checkResponse(response)) {
                redisUtils.setExpire(key);
                tripInfoCounter++;

                //Insert a composite key that allows reverse lookup of the dvj id
                //The format is route-direction-date-time
                final String joreKey = TransitdataProperties.formatJoreId(resultSet.getString(queryUtils.ROUTE_NAME),
                        resultSet.getString(queryUtils.DIRECTION), resultSet.getString(queryUtils.OPERATING_DAY),
                        resultSet.getString(queryUtils.START_TIME));
                response = redisUtils.setValue(joreKey, resultSet.getString(queryUtils.DVJ_ID));
                if (redisUtils.checkResponse(response)) {
                    redisUtils.setExpire(joreKey);
                    lookupCounter++;
                } else {
                    log.error("Failed to set reverse-lookup key {}, Redis returned {}", joreKey, response);
                }
            } else {
                log.error("Failed to set Trip details for key {}, Redis returned {}", key, response);
            }
        }

        log.info("Inserted {} trip info and {} reverse-lookup keys for {} DB rows", tripInfoCounter, lookupCounter, rowCounter);
    }

    protected String getQuery() {
        String query = new StringBuilder()
                .append("SELECT ")
                .append("   DISTINCT CONVERT(CHAR(16), DVJ.Id) AS " + queryUtils.DVJ_ID + ", ")
                .append("   KVV.StringValue AS " + queryUtils.ROUTE_NAME + ", ")
                .append("   SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1) AS " + queryUtils.DIRECTION + ", ")
                .append("   CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS " + queryUtils.OPERATING_DAY + ", ")
                .append("   RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2) ")
                .append("       + ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime)) ")
                .append("       - ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS " + queryUtils.START_TIME + " ")
                .append("FROM ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ ")
                .append("LEFT JOIN ptDOI4_Community.dbo.VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id) ")
                .append("LEFT JOIN ptDOI4_Community.T.KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number) ")
                .append("WHERE ")
                .append("   ( ")
                .append("       KT.Name = 'JoreIdentity' ")
                .append("       OR KT.Name = 'JoreRouteIdentity' ")
                .append("       OR KT.Name = 'RouteName' ")
                .append("   ) ")
                .append("   AND OT.Name = 'VehicleJourney' ")
                .append("   AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL ")
                .append("   AND DVJ.OperatingDayDate >= '" + queryUtils.from + "' ")
                .append("   AND DVJ.OperatingDayDate < '" + queryUtils.to + "' ")
                .append("   AND DVJ.IsReplacedById IS NULL ")
                .toString();
        return query;
    }
}
