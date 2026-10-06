package fi.hsl.transitdata.pubtransredisconnect;

import static fi.hsl.transitdata.pubtransredisconnect.RedisStoreMocks.succeedingRedisStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import fi.hsl.common.redis.RedisStore;
import fi.hsl.transitdata.pubtransredisconnect.MetroJourneyResultSetProcessor.MetroJourneyResultItem;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class MetroJourneyResultSetProcessorTest {

    private static final Duration TTL = Duration.ofDays(3);
    private static final MetroJourneyResultItem ITEM = new MetroJourneyResultItem("9187251000999999", "31M1", "1",
            "20261006", "05:30:00", "1020453");

    private RedisStore redisStore;
    private QueryUtils queryUtils;
    private MetroJourneyResultSetProcessor processor;

    @Before
    public void setUp() {
        redisStore = succeedingRedisStore();
        queryUtils = new QueryUtils(3, 90);
        processor = new MetroJourneyResultSetProcessor(redisStore, queryUtils, TTL);
    }

    @Test
    public void queryIsUnchanged() {
        String expected = "SELECT "
                + "   DISTINCT CONVERT(CHAR(16), DVJ.Id) AS dvj_id, "
                + "   KVV.StringValue AS route, "
                + "   SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1) AS direction, "
                + "   CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS operating_day, "
                + "   RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2) "
                + "       + ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime)) "
                + "       - ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS start_time, "
                + "   CONVERT(CHAR(7), JPP.Number) AS stop_number "
                + "FROM ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ "
                + "LEFT JOIN ptDOI4_Community.dbo.VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id) "
                + "LEFT JOIN ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id) "
                + "LEFT JOIN ptDOI4_Community.T.KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id) "
                + "LEFT JOIN ptDOI4_Community.dbo.KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId) "
                + "LEFT JOIN ptDOI4_Community.dbo.KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId) "
                + "LEFT JOIN ptDOI4_Community.dbo.ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number) "
                + "LEFT JOIN ptDOI4_Community.dbo.JourneyPatternPoint AS JPP ON (VJT.StartsAtJourneyPatternPointGid = JPP.Gid) "
                + "WHERE    (        KT.Name = 'JoreIdentity'        OR KT.Name = 'JoreRouteIdentity'        OR KT.Name = 'RouteName'    ) "
                + "   AND OT.Name = 'VehicleJourney' "
                + "   AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL "
                + "   AND DVJ.OperatingDayDate >= '" + queryUtils.from + "' "
                + "   AND DVJ.OperatingDayDate < '" + queryUtils.to + "' "
                + "   AND DVJ.IsReplacedById IS NULL "
                + "   AND VJT.TransportModeCode = 'METRO' ";

        assertEquals(expected, processor.getQuery());
    }

    @Test
    public void collectResultsReadsAliasedColumns() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString("dvj_id")).thenReturn(ITEM.dvjId());
        when(resultSet.getString("route")).thenReturn(ITEM.routeName());
        when(resultSet.getString("direction")).thenReturn(ITEM.direction());
        when(resultSet.getString("operating_day")).thenReturn(ITEM.operatingDay());
        when(resultSet.getString("start_time")).thenReturn(ITEM.startTime());
        when(resultSet.getString("stop_number")).thenReturn(ITEM.stopNumber());

        List<MetroJourneyResultItem> items = new ArrayList<>(processor.collectResults(resultSet));

        assertEquals(List.of(ITEM), items);
    }

    @Test
    public void collectResultsOfEmptyResultSetIsEmpty() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(false);

        assertTrue(processor.collectResults(resultSet).isEmpty());
    }

    @Test
    public void processItemsWritesMetroHashKeyedByStartStopAndUtcDateTime() throws Exception {
        processor.processItems(List.of(ITEM));

        verify(redisStore).setValues("metro:1020453_2026-10-06T05:30:00Z",
                Map.of("dvj-id", "9187251000999999", "route-name", "31M1", "direction", "1", "start-time",
                        "05:30:00", "operating-day", "20261006", "start-datetime", "2026-10-06T05:30:00Z",
                        "start-stop-number", "1020453"));
        verify(redisStore).setExpire("metro:1020453_2026-10-06T05:30:00Z", TTL);
    }

    @Test
    public void startTimePastMidnightRollsOverToNextUtcDay() throws Exception {
        processor.processItems(
                List.of(new MetroJourneyResultItem("1", "31M2", "2", "20261006", "25:10:00", "1020454")));

        verify(redisStore).setValues(eq("metro:1020454_2026-10-07T01:10:00Z"), any());
    }

    @Test
    public void dateTimeIgnoresDaylightSavingTransitions() throws Exception {
        // Treated as UTC, not Europe/Helsinki: the DST switch day gives the same wall-clock instant
        processor.processItems(
                List.of(new MetroJourneyResultItem("1", "31M1", "1", "20261025", "05:30:00", "1020453")));

        verify(redisStore).setValues(eq("metro:1020453_2026-10-25T05:30:00Z"), any());
    }

    @Test
    public void failedWriteSkipsExpiry() throws Exception {
        when(redisStore.setValues(anyString(), any())).thenReturn(null);

        processor.processItems(List.of(ITEM));

        verify(redisStore, never()).setExpire(anyString(), any());
    }

    @Test
    public void malformedOperatingDayAbortsProcessing() {
        MetroJourneyResultItem malformed = new MetroJourneyResultItem("1", "31M1", "1", "2026-10-06", "05:30:00",
                "1020453");

        assertThrows(DateTimeParseException.class, () -> processor.processItems(List.of(malformed, ITEM)));
        verify(redisStore, never()).setValues(anyString(), any());
    }
}
