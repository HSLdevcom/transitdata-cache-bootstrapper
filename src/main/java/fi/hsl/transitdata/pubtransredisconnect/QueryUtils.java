package fi.hsl.transitdata.pubtransredisconnect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class QueryUtils {

    private static final Logger log = LoggerFactory.getLogger(QueryUtils.class);

    public final String DVJ_ID = "dvj_id";
    public final String DIRECTION = "direction";
    public final String ROUTE_NAME = "route";
    public final String START_TIME = "start_time";
    public final String OPERATING_DAY = "operating_day";
    public final String STOP_NUMBER = "stop_number";

    private int queryHistoryInDays;
    private int queryFutureInDays;
    private int queryMinutesFromEvenHour;

    public String from;
    public String to;

    public QueryUtils(int queryHistoryInDays, int queryFutureInDays, int queryMinutesFromEvenHour) {
        this.queryHistoryInDays = queryHistoryInDays;
        this.queryFutureInDays = queryFutureInDays;
        this.queryMinutesFromEvenHour = queryMinutesFromEvenHour;
        this.updateFromToDates();
    }

    public void updateFromToDates() {
        this.from = formatDate(-queryHistoryInDays);
        this.to = formatDate(queryFutureInDays);
        log.info("Fetching data from {} to {}", this.from, this.to);
    }

    private static String formatDate(int offsetInDays) {
        LocalDate now = LocalDate.now();
        LocalDate then = now.plus(offsetInDays, ChronoUnit.DAYS);
        String formattedString = DateTimeFormatter.ISO_LOCAL_DATE.format(then);
        log.debug("offsetInDays results to date " + formattedString);
        return formattedString;
    }
    
    public long secondsUntilNextEvenHourPlusMinutes() {
        if (queryMinutesFromEvenHour < 0 || queryMinutesFromEvenHour > 59) {
            throw new IllegalArgumentException("Minutes must be between 0 and 59");
        }
        
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime thisHour = now.truncatedTo(ChronoUnit.HOURS);
        OffsetDateTime nextTime = thisHour.plusMinutes(queryMinutesFromEvenHour);
        
        if (nextTime.isBefore(now)) {
            OffsetDateTime nextHour = now.plusHours(1);
            OffsetDateTime evenHour = nextHour.truncatedTo(ChronoUnit.HOURS);
            nextTime = evenHour.plusMinutes(queryMinutesFromEvenHour);
        }
        
        log.debug("Current time is " + now.toString() + ", next time is at " + nextTime.toString());
        return Duration.between(now, nextTime).getSeconds();
    }
}
