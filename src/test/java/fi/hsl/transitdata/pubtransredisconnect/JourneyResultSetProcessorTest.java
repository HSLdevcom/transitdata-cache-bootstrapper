package fi.hsl.transitdata.pubtransredisconnect;

import static fi.hsl.transitdata.pubtransredisconnect.RedisStoreMocks.succeedingRedisStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import fi.hsl.common.redis.RedisStore;
import fi.hsl.transitdata.pubtransredisconnect.JourneyResultSetProcessor.JourneyResultItem;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class JourneyResultSetProcessorTest {

    private static final Duration TTL = Duration.ofDays(3);
    private static final JourneyResultItem ITEM = new JourneyResultItem("9187251000123456", "1069", "1", "20261006",
            "07:05:00");

    private RedisStore redisStore;
    private QueryUtils queryUtils;
    private JourneyResultSetProcessor processor;

    @Before
    public void setUp() {
        redisStore = succeedingRedisStore();
        queryUtils = new QueryUtils(3, 90);
        processor = new JourneyResultSetProcessor(redisStore, queryUtils, TTL);
    }

    @Test
    public void queryIsUnchanged() {
        String expected = "SELECT " + "   DISTINCT CONVERT(CHAR(16), DVJ.Id) AS dvj_id, "
                + "   KVV.StringValue AS route, "
                + "   SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1) AS direction, "
                + "   CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS operating_day, "
                + "   RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2) "
                + "       + ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime)) "
                + "       - ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS start_time "
                + "FROM ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ "
                + "LEFT JOIN ptDOI4_Community.dbo.VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id) "
                + "LEFT JOIN ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id) "
                + "LEFT JOIN ptDOI4_Community.T.KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id) "
                + "LEFT JOIN ptDOI4_Community.dbo.KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId) "
                + "LEFT JOIN ptDOI4_Community.dbo.KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId) "
                + "LEFT JOIN ptDOI4_Community.dbo.ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number) "
                + "WHERE    (        KT.Name = 'JoreIdentity'        OR KT.Name = 'JoreRouteIdentity'        OR KT.Name = 'RouteName'    ) "
                + "   AND OT.Name = 'VehicleJourney' " + "   AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL "
                + "   AND DVJ.OperatingDayDate >= '" + queryUtils.from + "' " + "   AND DVJ.OperatingDayDate < '"
                + queryUtils.to + "' " + "   AND DVJ.IsReplacedById IS NULL ";

        assertEquals(expected, processor.getQuery());
    }

    @Test
    public void queryUsesCurrentQueryWindow() {
        queryUtils.from = "2000-01-01";
        queryUtils.to = "2000-01-02";

        String query = processor.getQuery();

        assertTrue(query.contains("DVJ.OperatingDayDate >= '2000-01-01'"));
        assertTrue(query.contains("DVJ.OperatingDayDate < '2000-01-02'"));
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

        List<JourneyResultItem> items = new ArrayList<>(processor.collectResults(resultSet));

        assertEquals(List.of(ITEM), items);
    }

    @Test
    public void collectResultsOfEmptyResultSetIsEmpty() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(false);

        assertTrue(processor.collectResults(resultSet).isEmpty());
    }

    @Test
    public void processItemsWritesTripDetailsAndReverseLookupWithTtl() throws Exception {
        processor.processItems(List.of(ITEM));

        InOrder order = inOrder(redisStore);
        order.verify(redisStore).setValues("dvj:9187251000123456",
                Map.of("route-name", "1069", "direction", "1", "start-time", "07:05:00", "operating-day", "20261006"));
        order.verify(redisStore).setExpire("dvj:9187251000123456", TTL);
        order.verify(redisStore).setValue("jore:1069-1-20261006-07:05:00", "9187251000123456");
        order.verify(redisStore).setExpire("jore:1069-1-20261006-07:05:00", TTL);
    }

    @Test
    public void processItemsKeepsStartTimesPastMidnight() throws Exception {
        processor.processItems(List.of(new JourneyResultItem("1", "550", "2", "20261006", "25:10:00")));

        verify(redisStore).setValue("jore:550-2-20261006-25:10:00", "1");
    }

    @Test
    public void failedTripDetailsWriteSkipsExpiryAndReverseLookup() throws Exception {
        when(redisStore.setValues(eq("dvj:9187251000123456"), any())).thenReturn("ERR");

        processor.processItems(List.of(ITEM));

        verify(redisStore, never()).setExpire(anyString(), any());
        verify(redisStore, never()).setValue(anyString(), anyString());
    }

    @Test
    public void failedReverseLookupWriteSkipsItsExpiry() throws Exception {
        when(redisStore.setValue("jore:1069-1-20261006-07:05:00", "9187251000123456")).thenReturn(null);

        processor.processItems(List.of(ITEM));

        verify(redisStore).setExpire("dvj:9187251000123456", TTL);
        verify(redisStore, never()).setExpire("jore:1069-1-20261006-07:05:00", TTL);
    }

    @Test
    public void processesEveryItemAcrossProgressLogBatches() throws Exception {
        List<JourneyResultItem> items = new ArrayList<>();
        for (int i = 0; i < 5001; i++) {
            items.add(new JourneyResultItem(String.valueOf(i), "1069", "1", "20261006", "07:05:00"));
        }

        processor.processItems(items);

        verify(redisStore, times(5001)).setValues(anyString(), any());
        verify(redisStore, times(10002)).setExpire(anyString(), eq(TTL));
    }

    @Test
    public void processItemsOfEmptyCollectionWritesNothing() throws Exception {
        processor.processItems(List.of());

        verify(redisStore, never()).setValues(anyString(), any());
    }
}
