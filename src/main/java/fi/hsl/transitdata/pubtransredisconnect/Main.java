package fi.hsl.transitdata.pubtransredisconnect;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.redis.RedisStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;

import static fi.hsl.common.transitdata.TransitdataProperties.KEY_LAST_CACHE_UPDATE_TIMESTAMP;
import static java.time.OffsetTime.now;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static java.util.UUID.randomUUID;
import static org.slf4j.MDC.putCloseable;

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
            final var queryProcessor = new QueryProcessor(connection);
            final var journeyResultSetProcessor = new JourneyResultSetProcessor(redisStore, queryUtils, redisTtl);
            final var stopResultSetProcessor = new StopResultSetProcessor(redisStore, queryUtils, redisTtl);
            final var metroJourneyResultSetProcessor = new MetroJourneyResultSetProcessor(redisStore, queryUtils, redisTtl);

            queryProcessor.executeAndProcessQuery(journeyResultSetProcessor);
            queryProcessor.executeAndProcessQuery(stopResultSetProcessor);
            queryProcessor.executeAndProcessQuery(metroJourneyResultSetProcessor);

            updateTimestamp();

            log.info("All data processed");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        var jobId = randomUUID().toString();

        try (var ignored = putCloseable("jobId", jobId)) {
            log.info("Starting job {}", jobId);
            var config = ConfigParser.createConfig();

            try (var app = PulsarApplication.newInstance(config)) {
                var context = app.getContext();
                var main = new Main(context, System.getenv("TRANSITDATA_PUBTRANS_CONN_STRING"));
                main.start();
                log.info("PulsarApplication started successfully");
            }

            log.info("Job completed successfully");
        } catch (Exception e) {
            log.error("Job failed", e);
            System.exit(1);
            return;
        }

        System.exit(0);
    }

    private void updateTimestamp() {
        redisStore.execute(jedis -> {
            final var timestamp = ISO_INSTANT.format(now());
            log.info("Updating Redis with latest timestamp: " + timestamp);
            final var result = jedis.set(KEY_LAST_CACHE_UPDATE_TIMESTAMP, timestamp);
            if (!redisStore.checkResponse(result)) {
                log.error("Failed to update cache timestamp to Redis!");
            }
            return result;
        });
    }
}
