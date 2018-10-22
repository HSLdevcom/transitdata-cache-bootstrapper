package fi.hsl.transitdata.pubtransredisconnect;

import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.io.File;
import java.sql.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.microsoft.sqlserver.jdbc.*;
import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String DVJ_ID = "dvj_id";
    private static final String DIRECTION = "direction";
    private static final String ROUTE_NAME = "route";
    private static final String START_TIME = "start_time";
    private static final String OPERATING_DAY = "operating_day";

    Config config;

    private Jedis jedis;
    private final String connectionString;

    private AtomicBoolean processingActive = new AtomicBoolean(false);

    private int redisTTLInSeconds;
    private int queryFutureInDays;
    private int queryHistoryInDays;

    public Main(Config config, String connectionString) {
        this.config = config;
        this.connectionString = connectionString;
    }

    public void start() {
        initialize();

        startPolling();
        //Invoke manually the first task immediately
        process();

    }

    private void initialize() {
        final String redisHost = config.getString("redis.host");
        log.info("Connecting to redis at " + redisHost);
        jedis = new Jedis(redisHost);

        redisTTLInSeconds = config.getInt("bootstrapper.redisTTLInDays") * 24 * 60 * 60;
        log.info("Redis TTL in secs: " + redisTTLInSeconds);

        queryHistoryInDays = config.getInt("bootstrapper.queryHistoryInDays");
        queryFutureInDays = config.getInt("bootstrapper.queryFutureInDays");
        log.info("Fetching data from -" + queryHistoryInDays + " days to +" + queryFutureInDays + " days");

    }

    private long secondsUntilNextEvenHour() {
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime nextHour = now.plusHours(1);
        OffsetDateTime evenHour = nextHour.truncatedTo(ChronoUnit.HOURS);

        log.debug("Current time is " + now.toString() + ", next even hour is at " + evenHour.toString());

        return Duration.between(now, evenHour).getSeconds();
    }

    private void startPolling() {
        final long periodInSecs = 60 * 60;
        final long delayInSecs = secondsUntilNextEvenHour();

        log.info("Starting scheduled poll task. First poll execution in " + delayInSecs + "secs");
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                log.info("Poll timer tick");
                process();
            }
        };

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(task, delayInSecs, periodInSecs, TimeUnit.SECONDS);
    }

    private void process() {
        if (!processingActive.getAndSet(true)) {
            log.info("Fetching data");
            try (Connection connection = DriverManager.getConnection(connectionString)) {
                queryJourneyData(connection, jedis);
                queryStopData(connection, jedis);
            }
            catch (SQLException e) {
                log.error("SQL Exception: ", e);
            }
            processingActive.set(false);
            log.info("All data processed, thank you.");
        }
        else {
            log.warn("Processing already active, will not launch another task.");
        }
    }

    private String formatDate(int offsetInDays) {
        LocalDate now = LocalDate.now();
        LocalDate then = now.plus(offsetInDays, ChronoUnit.DAYS);

        String formattedString = DateTimeFormatter.ISO_LOCAL_DATE.format(then);
        log.debug("offsetInDays results to date " + formattedString);

        return formattedString;
    }

    private void queryJourneyData(Connection connection, Jedis jedis) throws SQLException {

        Statement statement;
        ResultSet resultSet;

        final String from = formatDate(-queryHistoryInDays);
        final String to = formatDate(queryFutureInDays);

        String selectSql = new StringBuilder()
                .append("SELECT ")
                .append(" CONVERT(CHAR(16), DVJ.Id) AS " + DVJ_ID + ",")
                .append(" KVV.StringValue AS " + ROUTE_NAME + ",")
                .append(" SUBSTRING(")
                .append("    CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid),")
                .append("       12,")
                .append("       1")
                .append("   ) AS " + DIRECTION + ",")
                .append(" CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS " + OPERATING_DAY + ", ")
                .append(" RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2) ")
                .append(" + ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime))")
                .append("- ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS " + START_TIME + " ")
                .append(" FROM")
                .append("     ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ,")
                .append("     ptDOI4_Community.dbo.VehicleJourney AS VJ,")
                .append("     ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT,")
                .append("     ptDOI4_Community.T.KeyVariantValue AS KVV,")
                .append("     ptDOI4_Community.dbo.KeyType AS KT,")
                .append("     ptDOI4_Community.dbo.KeyVariantType AS KVT,")
                .append("     ptDOI4_Community.dbo.ObjectType AS OT")
                .append(" WHERE")
                .append("     DVJ.IsBasedOnVehicleJourneyId = VJ.Id")
                .append("     AND DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id")
                .append("     AND (")
                .append("         KT.Name = 'JoreIdentity'")
                .append("         OR KT.Name = 'JoreRouteIdentity'")
                .append("         OR KT.Name = 'RouteName'")
                .append("     )")
                .append("     AND KT.ExtendsObjectTypeNumber = OT.Number")
                .append("     AND OT.Name = 'VehicleJourney'")
                .append("     AND KT.Id = KVT.IsForKeyTypeId")
                .append("     AND KVT.Id = KVV.IsOfKeyVariantTypeId")
                .append("     AND KVV.IsForObjectId = VJ.Id")
                .append("     AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL")
                .append("     AND DVJ.OperatingDayDate >= '" + from + "'")
                .append("     AND DVJ.OperatingDayDate < '" + to + "'")
                .append("     AND DVJ.IsReplacedById IS NULL")
                .toString();

        log.info("Starting journey query");
        long now = System.currentTimeMillis();

        statement = connection.createStatement();
        resultSet = statement.executeQuery(selectSql);

        try {
            handleJourneyResultSet(resultSet, jedis);
        }
        catch (Exception e) {
            log.error("Failed to handle result set", e);
        }
        long elapsed = (System.currentTimeMillis() - now) / 1000;
        log.info("Data handled in " + elapsed + " seconds");

        if (resultSet != null)  try { resultSet.close(); } catch (Exception e) {
            log.error("Exception while closing resultset", e);
        }
        if (statement != null)  try { statement.close(); } catch (Exception e) {
            log.error("Exception while closing statement", e);
        }

    }


    private void queryStopData(Connection connection, Jedis jedis) throws SQLException {

        Statement statement;
        ResultSet resultSet;

        String selectSql = new StringBuilder()
                .append("SELECT ")
                .append("[Gid], [Number] ")
                .append("FROM [ptDOI4_Community].[dbo].[JourneyPatternPoint] AS JPP ")
                .append("GROUP BY JPP.Gid, JPP.Number")
                .toString();

        log.info("Starting stop query");
        long now = System.currentTimeMillis();

        statement = connection.createStatement();
        resultSet = statement.executeQuery(selectSql);

        try {
            handleStopResultSet(resultSet, jedis);
            updateTimestamp(jedis);
        }
        catch (Exception e) {
            log.error("Exception while handling resultset", e);
        }
        long elapsed = (System.currentTimeMillis() - now) / 1000;
        log.info("Data handled in " + elapsed + " seconds");

        if (resultSet != null)  try { resultSet.close(); } catch (Exception e) {
            log.error("Exception while closing resultset", e);
        }
        if (statement != null)  try { statement.close(); } catch (Exception e) {
            log.error("Exception while closing statement", e);
        }

    }

    void updateTimestamp(Jedis jedis) {
        OffsetDateTime now = OffsetDateTime.now();
        String ts = DateTimeFormatter.ISO_INSTANT.format(now);
        log.info("Updating Redis with latest timestamp: " + ts);
        jedis.set(TransitdataProperties.KEY_LAST_CACHE_UPDATE_TIMESTAMP, ts);
    }

    private void handleJourneyResultSet(ResultSet resultSet, Jedis jedis) throws Exception {

        int rowCounter = 0;

        while(resultSet.next()) {
            Map<String, String> values = new HashMap<>();
            values.put(TransitdataProperties.KEY_ROUTE_NAME, resultSet.getString(ROUTE_NAME));
            values.put(TransitdataProperties.KEY_DIRECTION, resultSet.getString(DIRECTION));
            values.put(TransitdataProperties.KEY_START_TIME, resultSet.getString(START_TIME));
            values.put(TransitdataProperties.KEY_OPERATING_DAY, resultSet.getString(OPERATING_DAY));

            String key = TransitdataProperties.REDIS_PREFIX_DVJ + resultSet.getString(DVJ_ID);
            jedis.hmset(key, values);
            jedis.expire(key, redisTTLInSeconds);

            //Insert a composite key that allows reverse lookup of the dvj id
            //The format is route-direction-date-time
            String joreKey = TransitdataProperties.REDIS_PREFIX_JORE_ID + "-" + resultSet.getString(ROUTE_NAME) + "-" +
                    resultSet.getString(DIRECTION) + "-" + resultSet.getString(OPERATING_DAY) + "-" +
                    resultSet.getString(START_TIME);
            jedis.set(joreKey, resultSet.getString(DVJ_ID));
            jedis.expire(joreKey, redisTTLInSeconds);

            rowCounter++;
        }

        log.info("Inserted " + rowCounter + " dvj keys");
    }

    private void handleStopResultSet(ResultSet resultSet, Jedis jedis) throws Exception {

        int rowCounter = 0;

        while(resultSet.next()) {
            String key = TransitdataProperties.REDIS_PREFIX_JPP  + resultSet.getString(1);
            jedis.setex(key, redisTTLInSeconds, resultSet.getString(2));

            rowCounter++;
        }

        log.info("Inserted " + rowCounter + " jpp keys");
    }


    public static void main(String[] args) {

        String connectionString = "";

        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String secretFilePath = ConfigUtils.getEnv("FILEPATH_CONNECTION_STRING").orElse("/run/secrets/pubtrans_community_conn_string");
            connectionString = new Scanner(new File(secretFilePath))
                    .useDelimiter("\\Z").next();
        } catch (Exception e) {
            log.error("Failed to read the DB connection string from the file", e);
        }

        if (connectionString.equals("")) {
            log.error("Connection string empty, aborting.");
            System.exit(1);
        }
        Config config = ConfigParser.createConfig();

        Main app = new Main(config, connectionString);
        app.start();
    }

}
