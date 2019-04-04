package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.gtfsrt.JoreDateTime;
import fi.hsl.common.metro.MetroStops;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

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

public class MetroJourneyResultSetProcessor implements IResultSetProcessor {

    private static final Logger log = LoggerFactory.getLogger(MetroJourneyResultSetProcessor.class);

    private Jedis jedis;
    private int redisTTLInSeconds;

    public MetroJourneyResultSetProcessor(final Jedis jedis, final int redisTTLInSeconds) {
        this.jedis = jedis;
        this.redisTTLInSeconds = redisTTLInSeconds;
    }

    public void process(final ResultSet resultSet) throws Exception {
        int rowCounter = 0;

        while (resultSet.next()) {
            final String operatingDay = resultSet.getString(QueryUtil.OPERATING_DAY);
            final String startTime = resultSet.getString(QueryUtil.START_TIME);
            final String dateTime = processDateTime(operatingDay, startTime);
            final String stopNumber = resultSet.getString(QueryUtil.STOP_NUMBER);
            final Optional<String> maybeShortName = MetroStops.getShortName(stopNumber);

            if (maybeShortName.isPresent()) {
                final String shortName = maybeShortName.get();
                Map<String, String> values = new HashMap<>();
                values.put(TransitdataProperties.KEY_ROUTE_NAME, resultSet.getString(QueryUtil.ROUTE_NAME));
                values.put(TransitdataProperties.KEY_DIRECTION, resultSet.getString(QueryUtil.DIRECTION));
                values.put(TransitdataProperties.KEY_START_TIME, startTime);
                values.put(TransitdataProperties.KEY_OPERATING_DAY, operatingDay);
                values.put(TransitdataProperties.KEY_START_DATETIME, dateTime);
                values.put(TransitdataProperties.KEY_START_STOP_NUMBER, stopNumber);
                values.put(TransitdataProperties.KEY_START_STOP_SHORT_NAME, shortName);

                String metroKey = TransitdataProperties.formatMetroId(shortName, dateTime);
                jedis.hmset(metroKey, values);
                jedis.expire(metroKey, redisTTLInSeconds);

                rowCounter++;
            } else {
                log.warn("Failed to short name for stop number {}.", stopNumber);
            }
        }

        log.info("Inserted " + rowCounter + " metro keys");
    }

    private static String processDateTime(final String operatingDay, final String startTime) throws ParseException {
        LocalDate date = LocalDate.parse(operatingDay, DateTimeFormatter.BASIC_ISO_DATE);
        ZonedDateTime dateTime = ZonedDateTime.of(date, LocalTime.MIN, ZoneOffset.UTC);
        dateTime = dateTime.plus(JoreDateTime.timeStringToSeconds(startTime), ChronoUnit.SECONDS);
        String dateTimeString = DateTimeFormatter.ISO_INSTANT.format(dateTime).replace("Z", ".000Z");
        return dateTimeString;
    }
}
