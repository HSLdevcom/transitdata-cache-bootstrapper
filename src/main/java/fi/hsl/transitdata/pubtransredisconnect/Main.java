package fi.hsl.transitdata.pubtransredisconnect;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.function.Function;

import static fi.hsl.transitdata.pubtransredisconnect.Checks.checkEither;
import static fi.hsl.transitdata.pubtransredisconnect.RedisClusterProperties.redisClusterProperties;
import static redis.clients.jedis.Protocol.DEFAULT_DATABASE;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private final Config config;

    private final PulsarApplicationContext context;
    private final String connectionString;

    private RedisUtils redisUtils;
    private QueryUtils queryUtils;

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
        redisUtils = new RedisUtils(context, createJedisExecutor());
        final int queryHistoryInDays = config.getInt("bootstrapper.queryHistoryInDays");
        final int queryFutureInDays = config.getInt("bootstrapper.queryFutureInDays");
        log.info("Fetching data from -{} days to +{} days.",
                queryHistoryInDays, queryFutureInDays);
        queryUtils = new QueryUtils(queryHistoryInDays, queryFutureInDays);
    }

    private void process() {
        log.info("Fetching data");
        try (Connection connection = DriverManager.getConnection(connectionString)) {
            final QueryProcessor queryProcessor = new QueryProcessor(connection);
            final JourneyResultSetProcessor journeyResultSetProcessor = new JourneyResultSetProcessor(redisUtils, queryUtils);
            final StopResultSetProcessor stopResultSetProcessor = new StopResultSetProcessor(redisUtils, queryUtils);
            final MetroJourneyResultSetProcessor metroJourneyResultSetProcessor = new MetroJourneyResultSetProcessor(redisUtils, queryUtils);

            queryProcessor.firstExecuteQueryThenReleaseDbResourcesAndThenHandleResults(journeyResultSetProcessor);
            queryProcessor.executeAndProcessQuery(stopResultSetProcessor);
            queryProcessor.executeAndProcessQuery(metroJourneyResultSetProcessor);

            redisUtils.updateTimestamp();

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

    private JedisExecutor createJedisExecutor() {
        final var config = context.getConfig();
        final var redisEnabled = config.getBoolean("redis.enabled");
        final var redisClusterEnabled = config.getBoolean("redisCluster.enabled");
        checkEither(redisEnabled, redisClusterEnabled,
                "Exactly one of 'redis.enabled' or 'redisCluster.enabled' must be true");

        if (redisEnabled) {
            final var jedis = context.getJedis();
            return new JedisExecutor() {
                @Override
                public <T> T execute(Function<Jedis, T> action) {
                    synchronized (jedis) {
                        return action.apply(jedis);
                    }
                }
            };
        } else {
            final var pool = createJedisSentinelPool();

            return new JedisExecutor() {
                @Override
                public <T> T execute(Function<Jedis, T> action) {
                    try (final var jedis = pool.getResource()) {
                        return action.apply(jedis);
                    }
                }
            };
        }
    }

    private JedisSentinelPool createJedisSentinelPool() {
        final var properties = redisClusterProperties(config);

        return new JedisSentinelPool(
                properties.masterName,
                properties.sentinels,
                properties.jedisPoolConfig(),
                (int) properties.connectionTimeout.toMillis(),
                (int) properties.socketTimeout.toMillis(),
                null,
                DEFAULT_DATABASE
        );
    }
}
