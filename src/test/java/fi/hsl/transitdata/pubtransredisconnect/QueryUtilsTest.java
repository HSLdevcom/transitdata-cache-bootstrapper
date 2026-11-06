package fi.hsl.transitdata.pubtransredisconnect;

import static org.junit.Assert.assertEquals;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

public class QueryUtilsTest {

    private static String isoDate(int offsetInDays) {
        return DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDate.now().plusDays(offsetInDays));
    }

    @Test
    public void fromAndToAreIsoDatesRelativeToToday() {
        QueryUtils queryUtils = new QueryUtils(3, 90);

        assertEquals(isoDate(-3), queryUtils.from);
        assertEquals(isoDate(90), queryUtils.to);
    }

    @Test
    public void zeroOffsetsGiveTodayForBothBounds() {
        QueryUtils queryUtils = new QueryUtils(0, 0);

        assertEquals(isoDate(0), queryUtils.from);
        assertEquals(isoDate(0), queryUtils.to);
    }

    @Test
    public void updateFromToDatesRecomputesBounds() {
        QueryUtils queryUtils = new QueryUtils(2, 5);
        queryUtils.from = "stale";
        queryUtils.to = "stale";

        queryUtils.updateFromToDates();

        assertEquals(isoDate(-2), queryUtils.from);
        assertEquals(isoDate(5), queryUtils.to);
    }

    @Test
    public void columnAliasesAreStable() {
        QueryUtils queryUtils = new QueryUtils(1, 1);

        assertEquals("dvj_id", queryUtils.DVJ_ID);
        assertEquals("direction", queryUtils.DIRECTION);
        assertEquals("route", queryUtils.ROUTE_NAME);
        assertEquals("start_time", queryUtils.START_TIME);
        assertEquals("operating_day", queryUtils.OPERATING_DAY);
        assertEquals("stop_number", queryUtils.STOP_NUMBER);
    }
}
