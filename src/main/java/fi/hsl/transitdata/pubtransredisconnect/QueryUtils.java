package fi.hsl.transitdata.pubtransredisconnect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class QueryUtils {

    private static final Logger log = LoggerFactory.getLogger(QueryUtils.class);

    public final String DVJ_ID = "dvj_id";
    public final String DIRECTION = "direction";
    public final String ROUTE_NAME = "route";
    public final String START_TIME = "start_time";
    public final String OPERATING_DAY = "operating_day";
    public final String STOP_NUMBER = "stop_number";

    private final int queryHistoryInDays;
    private final int queryFutureInDays;

    public String from;
    public String to;

    public QueryUtils(int queryHistoryInDays, int queryFutureInDays) {
        this.queryHistoryInDays = queryHistoryInDays;
        this.queryFutureInDays = queryFutureInDays;
        this.updateFromToDates();
    }

    public void updateFromToDates() {
        this.from = formatDate(-queryHistoryInDays);
        this.to = formatDate(queryFutureInDays);
        log.info("Fetching data from {} to {}", this.from, this.to);
    }

    private static String formatDate(int offsetInDays) {
        LocalDate now = LocalDate.now();
        LocalDate then = now.plusDays(offsetInDays);
        String formattedString = DateTimeFormatter.ISO_LOCAL_DATE.format(then);
        log.debug("offsetInDays results to date {}", formattedString);
        return formattedString;
    }
}
