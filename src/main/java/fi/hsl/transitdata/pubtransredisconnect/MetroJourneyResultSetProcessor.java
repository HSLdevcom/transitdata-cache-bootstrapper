package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.redis.RedisStore;
import fi.hsl.common.transitdata.JoreDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import static fi.hsl.common.transitdata.TransitdataProperties.KEY_DIRECTION;
import static fi.hsl.common.transitdata.TransitdataProperties.KEY_DVJ_ID;
import static fi.hsl.common.transitdata.TransitdataProperties.KEY_OPERATING_DAY;
import static fi.hsl.common.transitdata.TransitdataProperties.KEY_ROUTE_NAME;
import static fi.hsl.common.transitdata.TransitdataProperties.KEY_START_DATETIME;
import static fi.hsl.common.transitdata.TransitdataProperties.KEY_START_STOP_NUMBER;
import static fi.hsl.common.transitdata.TransitdataProperties.KEY_START_TIME;
import static fi.hsl.common.transitdata.TransitdataProperties.formatMetroId;
import static fi.hsl.transitdata.pubtransredisconnect.MetroJourneyResultSetProcessor.MetroJourneyResultItem;

public class MetroJourneyResultSetProcessor extends AbstractResultSetProcessor<MetroJourneyResultItem> {

    private static final Logger log = LoggerFactory.getLogger(MetroJourneyResultSetProcessor.class);

    record MetroJourneyResultItem(String dvjId, String routeName, String direction, String operatingDay,
                                  String startTime, String stopNumber) {
    }

    public MetroJourneyResultSetProcessor(RedisStore redisStore, QueryUtils queryUtils, Duration redisTtl) {
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
                + queryUtils.START_TIME + ", " +
                "   CONVERT(CHAR(7), JPP.Number) AS " + queryUtils.STOP_NUMBER + " " +
                "FROM ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ " +
                "LEFT JOIN ptDOI4_Community.dbo.VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id) " +
                "LEFT JOIN ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id) " +
                "LEFT JOIN ptDOI4_Community.T.KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id) " +
                "LEFT JOIN ptDOI4_Community.dbo.KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId) " +
                "LEFT JOIN ptDOI4_Community.dbo.KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId) " +
                "LEFT JOIN ptDOI4_Community.dbo.ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number) " +
                "LEFT JOIN ptDOI4_Community.dbo.JourneyPatternPoint AS JPP ON (VJT.StartsAtJourneyPatternPointGid = JPP.Gid) " +
                "WHERE " + "   ( " + "       KT.Name = 'JoreIdentity' " +
                "       OR KT.Name = 'JoreRouteIdentity' " + "       OR KT.Name = 'RouteName' " +
                "   ) " + "   AND OT.Name = 'VehicleJourney' " +
                "   AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL " +
                "   AND DVJ.OperatingDayDate >= '" + queryUtils.from + "' " +
                "   AND DVJ.OperatingDayDate < '" + queryUtils.to + "' " +
                "   AND DVJ.IsReplacedById IS NULL " + "   AND VJT.TransportModeCode = 'METRO' ";
    }

    @Override
    Collection<MetroJourneyResultItem> collectResults(ResultSet resultSet) throws Exception {
        var items = new ArrayList<MetroJourneyResultItem>();

        while (resultSet.next()) {
            items.add(new MetroJourneyResultItem(
                    resultSet.getString(queryUtils.DVJ_ID),
                    resultSet.getString(queryUtils.ROUTE_NAME),
                    resultSet.getString(queryUtils.DIRECTION),
                    resultSet.getString(queryUtils.OPERATING_DAY),
                    resultSet.getString(queryUtils.START_TIME),
                    resultSet.getString(queryUtils.STOP_NUMBER)
            ));
        }

        return items;
    }

    @Override
    void processItems(Collection<MetroJourneyResultItem> items) throws Exception {
        int rowCounter = 0;
        int redisCounter = 0;

        for (var item : items) {
            rowCounter++;
            final var operatingDay = item.operatingDay;
            final var startTime = item.startTime;
            final var dateTime = processDateTime(operatingDay, startTime);
            final var stopNumber = item.stopNumber;

            final var values = new HashMap<String, String>();
            // remove fields that can be queried from MQTT
            values.put(KEY_DVJ_ID, item.dvjId);
            values.put(KEY_ROUTE_NAME, item.routeName);
            values.put(KEY_DIRECTION, item.direction);
            values.put(KEY_START_TIME, startTime);
            values.put(KEY_OPERATING_DAY, operatingDay);
            values.put(KEY_START_DATETIME, dateTime);
            values.put(KEY_START_STOP_NUMBER, stopNumber);

            var metroKey = formatMetroId(stopNumber, dateTime);
            var response = redisStore.setValues(metroKey, values);
            if (redisStore.checkResponse(response)) {
                redisStore.setExpire(metroKey, redisTtl);
                redisCounter++;
            } else {
                log.error("Failed to set metro key {}, Redis returned {}", metroKey, response);
            }
        }

        log.info("Inserted {} redis metro id keys for {} DB rows", redisCounter, rowCounter);
    }

    private static String processDateTime(final String operatingDay, final String startTime) throws ParseException {
        LocalDate date = LocalDate.parse(operatingDay, DateTimeFormatter.BASIC_ISO_DATE);
        ZonedDateTime dateTime = ZonedDateTime.of(date, LocalTime.MIN, ZoneOffset.UTC);
        dateTime = dateTime.plus(JoreDateTime.timeStringToSeconds(startTime), ChronoUnit.SECONDS);

        String dateTimeString = DateTimeFormatter.ISO_INSTANT.format(dateTime);

        return dateTimeString;
    }
}
