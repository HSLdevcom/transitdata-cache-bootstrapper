package fi.hsl.transitdata.pubtransredisconnect;

import static fi.hsl.transitdata.pubtransredisconnect.RedisStoreMocks.succeedingRedisStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import fi.hsl.common.redis.RedisStore;
import fi.hsl.transitdata.pubtransredisconnect.StopResultSetProcessor.StopResultItem;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class StopResultSetProcessorTest {

    private static final Duration TTL = Duration.ofDays(3);

    private RedisStore redisStore;
    private StopResultSetProcessor processor;

    @Before
    public void setUp() {
        redisStore = succeedingRedisStore();
        processor = new StopResultSetProcessor(redisStore, new QueryUtils(3, 90), TTL);
    }

    @Test
    public void queryGroupsAllJourneyPatternPointsWithoutDateFilter() {
        assertEquals("SELECT [Gid], [Number] FROM [ptDOI4_Community].[dbo].[JourneyPatternPoint] AS JPP "
                + "GROUP BY JPP.Gid, JPP.Number ", processor.getQuery());
    }

    @Test
    public void collectResultsReadsGidAndNumberColumns() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("Gid")).thenReturn("9025301000100001", "9025301000100002");
        when(resultSet.getString("Number")).thenReturn("1020453", "1020454");

        List<StopResultItem> items = new ArrayList<>(processor.collectResults(resultSet));

        assertEquals(List.of(new StopResultItem("9025301000100001", "1020453"),
                new StopResultItem("9025301000100002", "1020454")), items);
    }

    @Test
    public void collectResultsOfEmptyResultSetIsEmpty() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(false);

        assertTrue(processor.collectResults(resultSet).isEmpty());
    }

    @Test
    public void processItemsWritesJppKeyWithoutExpiry() throws Exception {
        processor.processItems(List.of(new StopResultItem("9025301000100001", "1020453")));

        verify(redisStore).setValue("jpp:9025301000100001", "1020453");
        // Stop keys are written without a TTL, unlike the journey keys
        verify(redisStore, never()).setExpire(anyString(), any());
    }

    @Test
    public void processItemsContinuesAfterFailedWrite() throws Exception {
        when(redisStore.setValue("jpp:1", "11")).thenReturn(null);

        processor.processItems(List.of(new StopResultItem("1", "11"), new StopResultItem("2", "22")));

        verify(redisStore).setValue("jpp:1", "11");
        verify(redisStore).setValue("jpp:2", "22");
    }

    @Test
    public void processItemsWritesNullStopNumberAsIs() throws Exception {
        processor.processItems(List.of(new StopResultItem("3", null)));

        verify(redisStore).setValue("jpp:3", null);
    }
}
