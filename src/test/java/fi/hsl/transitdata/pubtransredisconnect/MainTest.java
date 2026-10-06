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

import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.redis.RedisStore;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import redis.clients.jedis.Jedis;

/**
 * Runs the whole job as {@link Main#main} does (config from environment.conf, all three processors, timestamp
 * update) with the database behind a fake JDBC driver and Redis mocked.
 */
public class MainTest {

    private static final String JDBC_URL = "jdbc:cache-bootstrapper-test:pubtrans";

    private final List<String> executedQueries = new ArrayList<>();

    private FakeDriver driver;
    private Connection connection;
    private Statement statement;
    private RedisStore redisStore;
    private Jedis jedis;
    private PulsarApplicationContext context;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        connection = mock(Connection.class);
        statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenAnswer(invocation -> {
            executedQueries.add(invocation.getArgument(0));
            ResultSet empty = mock(ResultSet.class);
            when(empty.getStatement()).thenReturn(statement);
            return empty;
        });
        driver = new FakeDriver(connection);
        DriverManager.registerDriver(driver);

        jedis = mock(Jedis.class);
        when(jedis.set(anyString(), anyString())).thenReturn("OK");
        redisStore = succeedingRedisStore();
        when(redisStore.execute(any()))
                .thenAnswer(invocation -> ((Function<Jedis, Object>) invocation.getArgument(0)).apply(jedis));

        context = mock(PulsarApplicationContext.class);
        when(context.getConfig()).thenReturn(ConfigParser.createConfig());
        when(context.getRedisStore()).thenReturn(redisStore);
    }

    @After
    public void tearDown() throws Exception {
        DriverManager.deregisterDriver(driver);
    }

    @Test
    public void runsJourneyStopAndMetroQueriesInOrder() {
        new Main(context, JDBC_URL).start();

        assertEquals(3, executedQueries.size());
        assertTrue(executedQueries.get(0).contains("AS start_time FROM"));
        assertTrue(executedQueries.get(1).startsWith("SELECT [Gid], [Number]"));
        assertTrue(executedQueries.get(2).contains("VJT.TransportModeCode = 'METRO'"));
    }

    @Test
    public void queryWindowComesFromConfigDefaults() {
        new Main(context, JDBC_URL).start();

        QueryUtils expectedWindow = new QueryUtils(3, 90);
        assertTrue(executedQueries.get(0).contains("DVJ.OperatingDayDate >= '" + expectedWindow.from + "'"));
        assertTrue(executedQueries.get(0).contains("DVJ.OperatingDayDate < '" + expectedWindow.to + "'"));
    }

    @Test
    public void writesIsoInstantTimestampAfterProcessing() {
        Instant before = Instant.now();

        new Main(context, JDBC_URL).start();

        ArgumentCaptor<String> timestamp = ArgumentCaptor.forClass(String.class);
        verify(jedis).set(eq("cache-update-ts"), timestamp.capture());
        Instant written = Instant.parse(timestamp.getValue());
        assertTrue(!written.isBefore(before.minusSeconds(1)) && !written.isAfter(Instant.now()));
        assertTrue(timestamp.getValue().endsWith("Z"));
    }

    @Test
    public void closesDatabaseConnection() throws Exception {
        new Main(context, JDBC_URL).start();

        verify(connection).close();
    }

    @Test
    public void usesConfiguredTtlForJourneyKeys() throws Exception {
        ResultSet journeys = mock(ResultSet.class);
        when(journeys.next()).thenReturn(true, false);
        when(journeys.getString("dvj_id")).thenReturn("1");
        when(journeys.getString("route")).thenReturn("1069");
        when(journeys.getString("direction")).thenReturn("1");
        when(journeys.getString("operating_day")).thenReturn("20261006");
        when(journeys.getString("start_time")).thenReturn("07:05:00");
        when(statement.executeQuery(anyString())).thenReturn(journeys, mock(ResultSet.class), mock(ResultSet.class));

        new Main(context, JDBC_URL).start();

        verify(redisStore).setExpire("dvj:1", Duration.ofDays(3));
    }

    @Test
    public void failedQueryStillUpdatesTimestamp() throws Exception {
        // Pinned current behaviour: QueryProcessor swallows query errors, so the job reports success
        when(statement.executeQuery(anyString())).thenThrow(new SQLException("query failed"));

        new Main(context, JDBC_URL).start();

        verify(jedis).set(eq("cache-update-ts"), anyString());
    }

    @Test
    public void failedTimestampWriteIsOnlyLogged() {
        when(jedis.set(anyString(), anyString())).thenReturn("ERR");

        new Main(context, JDBC_URL).start();

        verify(jedis).set(eq("cache-update-ts"), anyString());
    }

    @Test
    public void unreachableDatabaseFailsWithoutWritingTimestamp() {
        Main main = new Main(context, "jdbc:no-such-driver:pubtrans");

        RuntimeException thrown = assertThrows(RuntimeException.class, main::start);

        assertTrue(thrown.getCause() instanceof SQLException);
        verify(redisStore, never()).execute(any());
    }

    @Test
    public void missingConnectionStringFailsWithoutWritingTimestamp() {
        Main main = new Main(context, null);

        assertThrows(RuntimeException.class, main::start);
        verify(redisStore, never()).execute(any());
    }

    private static final class FakeDriver implements Driver {

        private final Connection connection;

        private FakeDriver(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Connection connect(String url, Properties info) {
            return acceptsURL(url) ? connection : null;
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:cache-bootstrapper-test:");
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }
}
