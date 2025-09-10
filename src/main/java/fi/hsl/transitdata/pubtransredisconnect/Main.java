package fi.hsl.transitdata.pubtransredisconnect;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.redis.RedisStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import static fi.hsl.common.transitdata.TransitdataProperties.KEY_LAST_CACHE_UPDATE_TIMESTAMP;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private final Config config;

    private final PulsarApplicationContext context;
    private final String connectionString;

    private RedisStore redisStore;
    private QueryUtils queryUtils;
    private Duration redisTtl;

    public Main(PulsarApplicationContext context, String connectionString) {
        this.context = context;
        this.config = context.getConfig();
        this.connectionString = connectionString;
        this.UNHEALTHY_UPDATE_INTERVAL_SECS = config.getInt("application.unhealthyUpdateIntervalSecs");
        this.lastUpdateTime = System.currentTimeMillis();
    }

    final boolean lastUpdateTimeHealthy() {
        long updateIntervalMillis = System.currentTimeMillis() - lastUpdateTime;
        long intervalSecs = Math.round((double) updateIntervalMillis / 1000);
        if (intervalSecs > UNHEALTHY_UPDATE_INTERVAL_SECS) {
            log.error("Exceeded UNHEALTHY_UPDATE_INTERVAL_SECS threshold: {} s with interval of {} s",
                    UNHEALTHY_UPDATE_INTERVAL_SECS, intervalSecs);
            return false;
        }
        return true;
    }

    public void start() {
        initialize();
        process();
    }

    private void initialize() {
        final int queryHistoryInDays = config.getInt("bootstrapper.queryHistoryInDays");
        final int queryFutureInDays = config.getInt("bootstrapper.queryFutureInDays");
        log.info("Fetching data from -{} days to +{} days.",
                queryHistoryInDays, queryFutureInDays);
        redisStore = context.getRedisStore();
        redisTtl = Duration.ofDays(config.getInt("bootstrapper.redisTTLInDays"));
        queryUtils = new QueryUtils(queryHistoryInDays, queryFutureInDays);
    }

    private void process() {
        log.info("Fetching data");
        try (Connection connection = DriverManager.getConnection(connectionString)) {
            final QueryProcessor queryProcessor = new QueryProcessor(connection);
            final JourneyResultSetProcessor journeyResultSetProcessor = new JourneyResultSetProcessor(redisStore, queryUtils, redisTtl);
            final StopResultSetProcessor stopResultSetProcessor = new StopResultSetProcessor(redisStore, queryUtils, redisTtl);
            final MetroJourneyResultSetProcessor metroJourneyResultSetProcessor = new MetroJourneyResultSetProcessor(redisStore, queryUtils, redisTtl);

            queryProcessor.firstExecuteQueryThenReleaseDbResourcesAndThenHandleResults(journeyResultSetProcessor);
            queryProcessor.executeAndProcessQuery(stopResultSetProcessor);
            queryProcessor.executeAndProcessQuery(metroJourneyResultSetProcessor);

            updateTimestamp();

            log.info("All data processed, thank you.");
        } catch (SQLServerException sqlServerException) {
            String msg = "SQLServerException during query, Driver Error code: "
                    + sqlServerException.getErrorCode()
                    + " and SQL State: " + sqlServerException.getSQLState();
            log.error(msg, sqlServerException);
        } catch (Exception e) {
            log.error("Unknown exception during query ", e);
        }
    }

    public static void main(String[] args) {
        String connectionString = "";

        try {
            //The Default path is what works with Docker out-of-the-box. Override with a local file if needed
            connectionString = System.getenv("TRANSITDATA_PUBTRANS_CONN_STRING");
        } catch (Exception e) {
            log.error("Failed to read the DB connection string from the file", e);
        }

        if (connectionString.isEmpty()) {
            log.error("Connection string empty, aborting.");
            System.exit(1);
        }
        Config config = ConfigParser.createConfig();

        try (PulsarApplication app = PulsarApplication.newInstance(config)) {
            PulsarApplicationContext context = app.getContext();
            Main main = new Main(context, connectionString);
            main.start();
            log.info("PulsarApplication started successfully");
        } catch (Exception e) {
            log.error("Exception at main", e);
            System.exit(1);
        }

        log.info("Application completed successfully.");
        System.exit(0); // Exit with success code after successful execution
    }

    private void updateTimestamp() {
        redisStore.execute(jedis -> {
            final OffsetDateTime now = OffsetDateTime.now();
            final String ts = DateTimeFormatter.ISO_INSTANT.format(now);
            log.info("Updating Redis with latest timestamp: " + ts);
            final var result = jedis.set(KEY_LAST_CACHE_UPDATE_TIMESTAMP, ts);
            if (!redisStore.checkResponse(result)) {
                log.error("Failed to update cache timestamp to Redis!");
            }
            return result;
        });
    }
}
