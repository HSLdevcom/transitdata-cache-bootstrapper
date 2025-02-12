package fi.hsl.transitdata.pubtransredisconnect.processor;

import fi.hsl.common.transitdata.JoreDateTime;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.transitdata.pubtransredisconnect.util.QueryUtils;
import fi.hsl.transitdata.pubtransredisconnect.util.RedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class MetroJourneyResultSetProcessor extends AbstractResultSetProcessor {
    private static final Logger log = LoggerFactory.getLogger(MetroJourneyResultSetProcessor.class);

    public MetroJourneyResultSetProcessor(final RedisUtils redisUtils, final QueryUtils queryUtils) {
        super(redisUtils, queryUtils);
    }

    public void processResultSet(final ResultSet resultSet) throws Exception {
        int rowCounter = 0;
        int redisCounter = 0;

        while (resultSet.next()) {
            rowCounter++;
            final String operatingDay = resultSet.getString(queryUtils.OPERATING_DAY);
            final String startTime = resultSet.getString(queryUtils.START_TIME);
            final String dateTime = processDateTime(operatingDay, startTime);
            final String stopNumber = resultSet.getString(queryUtils.STOP_NUMBER);

            Map<String, String> values = new HashMap<>();
            // remove fields that can be queried from MQTT
            values.put(TransitdataProperties.KEY_DVJ_ID, resultSet.getString(queryUtils.DVJ_ID));
            values.put(TransitdataProperties.KEY_ROUTE_NAME, resultSet.getString(queryUtils.ROUTE_NAME));
            values.put(TransitdataProperties.KEY_DIRECTION, resultSet.getString(queryUtils.DIRECTION));
            values.put(TransitdataProperties.KEY_START_TIME, startTime);
            values.put(TransitdataProperties.KEY_OPERATING_DAY, operatingDay);
            values.put(TransitdataProperties.KEY_START_DATETIME, dateTime);
            values.put(TransitdataProperties.KEY_START_STOP_NUMBER, stopNumber);

            String metroKey = TransitdataProperties.formatMetroId(stopNumber, dateTime);
            String response = redisUtils.setValues(metroKey, values);
            if (redisUtils.checkResponse(response)) {
                redisUtils.setExpire(metroKey);
                redisCounter++;
            } else {
                log.error("Failed to set metro key {}, Redis returned {}", metroKey, response);
            }
        }

        log.info("Inserted {} redis metro id keys for {} DB rows", redisCounter, rowCounter);
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
                .append("       - ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS " + queryUtils.START_TIME + ", ")
                .append("   CONVERT(CHAR(7), JPP.Number) AS " + queryUtils.STOP_NUMBER + " ")
                .append("FROM ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ ")
                .append("LEFT JOIN ptDOI4_Community.dbo.VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id) ")
                .append("LEFT JOIN ptDOI4_Community.T.KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.JourneyPatternPoint AS JPP ON (VJT.StartsAtJourneyPatternPointGid = JPP.Gid) ")
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
                .append("   AND VJT.TransportModeCode = 'METRO' ")
                .toString();
        return query;
    }

    private static String processDateTime(final String operatingDay, final String startTime) throws ParseException {
        LocalDate date = LocalDate.parse(operatingDay, DateTimeFormatter.BASIC_ISO_DATE);
        ZonedDateTime dateTime = ZonedDateTime.of(date, LocalTime.MIN, ZoneOffset.UTC);
        dateTime = dateTime.plus(JoreDateTime.timeStringToSeconds(startTime), ChronoUnit.SECONDS);

        String dateTimeString = DateTimeFormatter.ISO_INSTANT.format(dateTime);

        return dateTimeString;
    }
}
