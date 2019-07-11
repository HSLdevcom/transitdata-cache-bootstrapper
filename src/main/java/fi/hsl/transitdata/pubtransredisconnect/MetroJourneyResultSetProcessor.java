package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.metro.MetroStops;
import fi.hsl.common.transitdata.JoreDateTime;
import fi.hsl.common.transitdata.TransitdataProperties;
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
import java.util.Optional;

public class MetroJourneyResultSetProcessor extends AbstractResultSetProcessor {
    private static final Logger log = LoggerFactory.getLogger(MetroJourneyResultSetProcessor.class);

    private String from;
    private String to;

    public MetroJourneyResultSetProcessor(final RedisUtils redisUtils, final String from, final String to) {
        super(redisUtils);
        this.from = from;
        this.to = to;
    }

    public void processResultSet(final ResultSet resultSet) throws Exception {
        int rowCounter = 0;
        int redisCounter = 0;

        while (resultSet.next()) {
            rowCounter++;
            final String operatingDay = resultSet.getString(QueryUtils.OPERATING_DAY);
            final String startTime = resultSet.getString(QueryUtils.START_TIME);
            final String dateTime = processDateTime(operatingDay, startTime);
            final String stopNumber = resultSet.getString(QueryUtils.STOP_NUMBER);
            final Optional<String> maybeShortName = MetroStops.getShortName(stopNumber);

            if (maybeShortName.isPresent()) {
                final String shortName = maybeShortName.get();
                Map<String, String> values = new HashMap<>();
                values.put(TransitdataProperties.KEY_DVJ_ID, resultSet.getString(QueryUtils.DVJ_ID));
                values.put(TransitdataProperties.KEY_ROUTE_NAME, resultSet.getString(QueryUtils.ROUTE_NAME));
                values.put(TransitdataProperties.KEY_DIRECTION, resultSet.getString(QueryUtils.DIRECTION));
                values.put(TransitdataProperties.KEY_START_TIME, startTime);
                values.put(TransitdataProperties.KEY_OPERATING_DAY, operatingDay);
                values.put(TransitdataProperties.KEY_START_DATETIME, dateTime);
                values.put(TransitdataProperties.KEY_START_STOP_NUMBER, stopNumber);
                values.put(TransitdataProperties.KEY_START_STOP_SHORT_NAME, shortName);

                String metroKey = TransitdataProperties.formatMetroId(shortName, dateTime);
                String response = redisUtils.setValues(metroKey, values);
                if (redisUtils.checkResponse(response)) {
                    redisUtils.setExpire(metroKey);
                    redisCounter++;
                } else {
                    log.error("Failed to set metro key {}, Redis returned {}", metroKey, response);
                }
            } else {
                log.warn("Failed to short name for stop number {}.", stopNumber);
            }
        }

        log.info("Inserted {} redis metro id keys for {} DB rows", redisCounter, rowCounter);
    }

    protected String getQuery() {
        String query = new StringBuilder()
                .append("SELECT ")
                .append("   DISTINCT CONVERT(CHAR(16), DVJ.Id) AS " + QueryUtils.DVJ_ID + ", ")
                .append("   KVV.StringValue AS " + QueryUtils.ROUTE_NAME + ", ")
                .append("   SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1) AS " + QueryUtils.DIRECTION + ", ")
                .append("   CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS " + QueryUtils.OPERATING_DAY + ", ")
                .append("   RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2) ")
                .append("       + ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime)) ")
                .append("       - ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS " + QueryUtils.START_TIME + ", ")
                .append("   CONVERT(CHAR(7), JPP.Number) AS " + QueryUtils.STOP_NUMBER + " ")
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
                .append("   AND DVJ.OperatingDayDate >= '" + from + "' ")
                .append("   AND DVJ.OperatingDayDate < '" + to + "' ")
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

        log.info("new dateTimeString " + dateTimeString);
        return dateTimeString;
    }
}
