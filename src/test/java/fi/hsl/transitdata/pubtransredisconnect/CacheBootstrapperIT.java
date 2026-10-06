package fi.hsl.transitdata.pubtransredisconnect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

/**
 * End-to-end test against a real SQL Server and a real Redis behind Sentinel: the job is wired exactly like
 * {@link Main} (config from environment.conf, {@link PulsarApplication}, all three processors) with only the
 * database connection string, the Sentinel address and the query window overridden.
 */
public class CacheBootstrapperIT {

    private static final DockerImageName MSSQL_IMAGE = DockerImageName
            .parse("mcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04");
    private static final String REDIS_IMAGE = "redis:7.4.11-alpine";
    private static final String MASTER_NAME = "mymaster";

    private static final LocalDate TODAY = LocalDate.now();
    private static final int HISTORY_DAYS = 3;
    private static final int FUTURE_DAYS = 5;

    private static final String BUS_DIRECTION_1_GID = "9011301069010002";
    private static final String BUS_DIRECTION_2_GID = "9011301069020002";
    private static final String METRO_DIRECTION_1_GID = "9011301031010002";
    private static final String BUS_STOP_GID = "9025301000100001";
    private static final String METRO_STOP_GID = "9025301000100453";

    private static MSSQLServerContainer<?> mssql;
    private static GenericContainer<?> redis;
    private static int redisPort;
    private static int sentinelPort;
    private static String connectionString;

    @BeforeClass
    @SuppressWarnings("deprecation")
    public static void startContainers() throws Exception {
        mssql = new MSSQLServerContainer<>(MSSQL_IMAGE).acceptLicense();
        mssql.start();
        connectionString = mssql.getJdbcUrl() + ";user=" + mssql.getUsername() + ";password=" + mssql.getPassword();
        createPubtransSchema();

        // Sentinel announces the master as 127.0.0.1:<redisPort>, so host and container ports must be the same
        redisPort = freePort();
        sentinelPort = freePort();
        String sentinelConf = "port " + sentinelPort + "\\nsentinel monitor " + MASTER_NAME + " 127.0.0.1 " + redisPort
                + " 1\\n";
        redis = new FixedHostPortGenericContainer<>(REDIS_IMAGE).withFixedExposedPort(redisPort, redisPort)
                .withFixedExposedPort(sentinelPort, sentinelPort)
                .withCommand("sh", "-c",
                        "redis-server --port " + redisPort + " --save '' --appendonly no --daemonize yes"
                                + " && printf '" + sentinelConf + "' > /tmp/sentinel.conf"
                                + " && exec redis-sentinel /tmp/sentinel.conf")
                .waitingFor(Wait.forLogMessage(".*\\+monitor master " + MASTER_NAME + ".*", 1));
        redis.start();
    }

    @AfterClass
    public static void stopContainers() {
        if (redis != null) {
            redis.stop();
        }
        if (mssql != null) {
            mssql.stop();
        }
    }

    @Before
    public void flushRedis() {
        try (Jedis jedis = redisClient()) {
            jedis.flushAll();
        }
    }

    @Test
    public void writesJourneyStopAndMetroKeysFromPubtrans() throws Exception {
        runInProcess(2);

        try (Jedis jedis = redisClient()) {
            String today = basicDate(0);
            String historyEdge = basicDate(-HISTORY_DAYS);

            assertEquals(
                    new TreeSet<>(List.of("dvj:9187251000000001", "dvj:9187251000000002", "dvj:9187251000000003",
                            "dvj:9187251000000009", "jore:1069-1-" + today + "-07:05:00",
                            "jore:550-2-" + today + "-25:10:00", "jore:31M1-1-" + today + "-05:30:00",
                            "jore:1069-1-" + historyEdge + "-07:05:00", "jpp:" + BUS_STOP_GID, "jpp:" + METRO_STOP_GID,
                            "metro:1020453_" + isoDate(0) + "T05:30:00Z", "cache-update-ts")),
                    new TreeSet<>(jedis.keys("*")));

            assertEquals(
                    Map.of("route-name", "1069", "direction", "1", "start-time", "07:05:00", "operating-day", today),
                    jedis.hgetAll("dvj:9187251000000001"));
            assertEquals(
                    Map.of("route-name", "550", "direction", "2", "start-time", "25:10:00", "operating-day", today),
                    jedis.hgetAll("dvj:9187251000000002"));
            assertEquals(
                    Map.of("route-name", "31M1", "direction", "1", "start-time", "05:30:00", "operating-day", today),
                    jedis.hgetAll("dvj:9187251000000003"));
            assertEquals(Map.of("route-name", "1069", "direction", "1", "start-time", "07:05:00", "operating-day",
                    historyEdge), jedis.hgetAll("dvj:9187251000000009"));

            assertEquals("9187251000000001", jedis.get("jore:1069-1-" + today + "-07:05:00"));
            assertEquals("9187251000000002", jedis.get("jore:550-2-" + today + "-25:10:00"));
            assertEquals("9187251000000003", jedis.get("jore:31M1-1-" + today + "-05:30:00"));

            assertEquals("1020001", jedis.get("jpp:" + BUS_STOP_GID));
            assertEquals("1020453", jedis.get("jpp:" + METRO_STOP_GID));

            assertEquals(
                    Map.of("dvj-id", "9187251000000003", "route-name", "31M1", "direction", "1", "start-time",
                            "05:30:00", "operating-day", today, "start-datetime", isoDate(0) + "T05:30:00Z",
                            "start-stop-number", "1020453"),
                    jedis.hgetAll("metro:1020453_" + isoDate(0) + "T05:30:00Z"));

            assertTtlDays(jedis, "dvj:9187251000000001", 2);
            assertTtlDays(jedis, "jore:1069-1-" + today + "-07:05:00", 2);
            assertTtlDays(jedis, "metro:1020453_" + isoDate(0) + "T05:30:00Z", 2);
            assertEquals("stop keys have no expiry", -1L, jedis.ttl("jpp:" + BUS_STOP_GID));
            assertEquals("timestamp has no expiry", -1L, jedis.ttl("cache-update-ts"));

            Instant timestamp = Instant.parse(jedis.get("cache-update-ts"));
            assertTrue(timestamp.isAfter(Instant.now().minus(Duration.ofMinutes(5))));
        }
    }

    @Test
    public void jobProcessExitsZeroAndHonoursEnvironmentVariables() throws Exception {
        int exitCode = runMainProcess(connectionString);

        assertEquals(0, exitCode);
        try (Jedis jedis = redisClient()) {
            assertNotNull(jedis.get("cache-update-ts"));
            assertTrue(jedis.exists("dvj:9187251000000001"));
            // QUERY_HISTORY_DAYS / REDIS_TTL_DAYS from the environment, as in the CronJob manifest
            assertTtlDays(jedis, "dvj:9187251000000001", 4);
        }
    }

    @Test
    public void jobProcessExitsOneWhenDatabaseIsUnreachable() throws Exception {
        String wrongPassword = mssql.getJdbcUrl() + ";user=" + mssql.getUsername() + ";password=wrong";

        int exitCode = runMainProcess(wrongPassword);

        assertEquals(1, exitCode);
        try (Jedis jedis = redisClient()) {
            assertFalse(jedis.exists("cache-update-ts"));
            assertTrue(jedis.keys("*").isEmpty());
        }
    }

    private static void runInProcess(int redisTtlDays) throws Exception {
        Config overrides = ConfigFactory.parseMap(Map.of("redisCluster.sentinels", "127.0.0.1:" + sentinelPort,
                "redisCluster.masterName", MASTER_NAME, "bootstrapper.redisTTLInDays", redisTtlDays,
                "bootstrapper.queryHistoryInDays", HISTORY_DAYS, "bootstrapper.queryFutureInDays", FUTURE_DAYS));
        Config config = overrides.withFallback(ConfigParser.createConfig()).resolve();

        try (PulsarApplication app = PulsarApplication.newInstance(config)) {
            new Main(app.getContext(), connectionString).start();
        }
    }

    private static int runMainProcess(String pubtransConnectionString) throws Exception {
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        ProcessBuilder builder = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                Main.class.getName());
        Map<String, String> env = builder.environment();
        env.put("TRANSITDATA_PUBTRANS_CONN_STRING", pubtransConnectionString);
        env.put("REDIS_CLUSTER_MASTER_NAME", MASTER_NAME);
        env.put("REDIS_CLUSTER_SENTINELS", "127.0.0.1:" + sentinelPort);
        env.put("REDIS_TTL_DAYS", "4");
        env.put("QUERY_HISTORY_DAYS", String.valueOf(HISTORY_DAYS));
        env.put("QUERY_FUTURE_DAYS", String.valueOf(FUTURE_DAYS));
        builder.redirectErrorStream(true);

        Process process = builder.start();
        byte[] output = process.getInputStream().readAllBytes();
        assertTrue("job did not finish", process.waitFor(2, TimeUnit.MINUTES));
        System.out.println(new String(output, StandardCharsets.UTF_8));
        return process.exitValue();
    }

    private static void createPubtransSchema() throws Exception {
        String masterUrl = mssql.getJdbcUrl() + ";user=" + mssql.getUsername() + ";password=" + mssql.getPassword();
        try (Connection connection = DriverManager.getConnection(masterUrl);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE ptDOI4_Community");
        }
        try (Connection connection = DriverManager.getConnection(masterUrl + ";databaseName=ptDOI4_Community");
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA T");
            statement.execute("CREATE TABLE dbo.DatedVehicleJourney (Id BIGINT PRIMARY KEY, "
                    + "IsBasedOnVehicleJourneyId BIGINT, IsBasedOnVehicleJourneyTemplateId BIGINT, "
                    + "OperatingDayDate DATE, IsReplacedById BIGINT NULL)");
            statement.execute("CREATE TABLE dbo.VehicleJourney (Id BIGINT PRIMARY KEY, "
                    + "PlannedStartOffsetDateTime DATETIME)");
            statement.execute("CREATE TABLE dbo.VehicleJourneyTemplate (Id BIGINT PRIMARY KEY, "
                    + "IsWorkedOnDirectionOfLineGid BIGINT NULL, StartsAtJourneyPatternPointGid BIGINT, "
                    + "TransportModeCode VARCHAR(16))");
            statement.execute("CREATE TABLE T.KeyVariantValue (IsForObjectId BIGINT, IsOfKeyVariantTypeId INT, "
                    + "StringValue VARCHAR(64))");
            statement.execute("CREATE TABLE dbo.KeyVariantType (Id INT PRIMARY KEY, IsForKeyTypeId INT)");
            statement.execute("CREATE TABLE dbo.KeyType (Id INT PRIMARY KEY, Name VARCHAR(64), "
                    + "ExtendsObjectTypeNumber INT)");
            statement.execute("CREATE TABLE dbo.ObjectType (Number INT PRIMARY KEY, Name VARCHAR(64))");
            statement.execute("CREATE TABLE dbo.JourneyPatternPoint (Gid BIGINT PRIMARY KEY, Number INT)");

            statement.execute("INSERT INTO dbo.ObjectType VALUES (10, 'VehicleJourney'), (20, 'Line')");
            statement.execute("INSERT INTO dbo.KeyType VALUES (1, 'RouteName', 10), (2, 'SomethingElse', 10), "
                    + "(3, 'RouteName', 20)");
            statement.execute("INSERT INTO dbo.KeyVariantType VALUES (1, 1), (2, 2), (3, 3)");
            statement.execute("INSERT INTO dbo.JourneyPatternPoint VALUES (" + BUS_STOP_GID + ", 1020001), ("
                    + METRO_STOP_GID + ", 1020453)");

            // Templates: bus direction 1 and 2, metro direction 1, and one without a direction
            statement.execute("INSERT INTO dbo.VehicleJourneyTemplate VALUES " + "(501, " + BUS_DIRECTION_1_GID + ", "
                    + BUS_STOP_GID + ", 'BUS'), " + "(502, " + BUS_DIRECTION_2_GID + ", " + BUS_STOP_GID + ", 'BUS'), "
                    + "(503, " + METRO_DIRECTION_1_GID + ", " + METRO_STOP_GID + ", 'METRO'), " + "(504, NULL, "
                    + BUS_STOP_GID + ", 'BUS')");
            statement.execute("INSERT INTO dbo.VehicleJourney VALUES "
                    + "(1001, '1900-01-01T07:05:00'), (1002, '1900-01-02T01:10:00'), "
                    + "(1003, '1900-01-01T05:30:00'), (1006, '1900-01-01T08:00:00'), "
                    + "(1007, '1900-01-01T09:00:00'), (1010, '1900-01-01T10:00:00')");
            statement.execute("INSERT INTO T.KeyVariantValue VALUES (1001, 1, '1069'), (1002, 1, '550'), "
                    + "(1003, 1, '31M1'), (1006, 2, 'not-a-route'), (1007, 3, 'line-key'), (1010, 1, '9999')");

            statement.execute("INSERT INTO dbo.DatedVehicleJourney VALUES "
                    // included: bus today, bus past midnight today, metro today, bus at the history edge
                    + "(9187251000000001, 1001, 501, '" + isoDate(0) + "', NULL), " + "(9187251000000002, 1002, 502, '"
                    + isoDate(0) + "', NULL), " + "(9187251000000003, 1003, 503, '" + isoDate(0) + "', NULL), "
                    + "(9187251000000009, 1001, 501, '" + isoDate(-HISTORY_DAYS) + "', NULL), "
                    // excluded: beyond the window, replaced, wrong key type, wrong object type, no direction,
                    // before the window, at the (exclusive) future edge
                    + "(9187251000000004, 1001, 501, '" + isoDate(FUTURE_DAYS + 5) + "', NULL), "
                    + "(9187251000000005, 1001, 501, '" + isoDate(0) + "', 9187251000000001), "
                    + "(9187251000000006, 1006, 501, '" + isoDate(0) + "', NULL), " + "(9187251000000010, 1007, 501, '"
                    + isoDate(0) + "', NULL), " + "(9187251000000011, 1010, 504, '" + isoDate(0) + "', NULL), "
                    + "(9187251000000007, 1001, 501, '" + isoDate(-HISTORY_DAYS - 1) + "', NULL), "
                    + "(9187251000000008, 1001, 501, '" + isoDate(FUTURE_DAYS) + "', NULL)");
        }
    }

    private static void assertTtlDays(Jedis jedis, String key, int days) {
        long ttl = jedis.ttl(key);
        long expected = Duration.ofDays(days).toSeconds();
        assertTrue(key + " TTL " + ttl + " not close to " + expected, ttl > expected - 300 && ttl <= expected);
    }

    private static Jedis redisClient() {
        return new Jedis("127.0.0.1", redisPort);
    }

    private static String isoDate(int offsetInDays) {
        return DateTimeFormatter.ISO_LOCAL_DATE.format(TODAY.plusDays(offsetInDays));
    }

    private static String basicDate(int offsetInDays) {
        return DateTimeFormatter.BASIC_ISO_DATE.format(TODAY.plusDays(offsetInDays));
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
