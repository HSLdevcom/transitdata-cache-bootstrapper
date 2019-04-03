package fi.hsl.transitdata.pubtransredisconnect;

import java.text.ParseException;
import java.time.*;
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
import java.util.stream.Collectors;

import com.microsoft.sqlserver.jdbc.*;
import com.typesafe.config.*;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.gtfsrt.JoreDateTime;
import fi.hsl.common.metro.MetroStops;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    Config config;

    private Jedis jedis;
    private final String connectionString;

    private ScheduledExecutorService executor;
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

        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(task, delayInSecs, periodInSecs, TimeUnit.SECONDS);
    }

    private void process() {
        if (!processingActive.getAndSet(true)) {
            log.info("Fetching data");
            try (Connection connection = DriverManager.getConnection(connectionString)) {
                final QueryProcessor journeyQueryProcessor = new QueryProcessor(connection, new JourneyResultSetProcessor(jedis, redisTTLInSeconds));
                final QueryProcessor stopQueryProcessor = new QueryProcessor(connection, new StopResultSetProcessor(jedis, redisTTLInSeconds));
                final QueryProcessor metroJourneyQueryProcessor = new QueryProcessor(connection, new MetroJourneyResultSetProcessor(jedis, redisTTLInSeconds));

                journeyQueryProcessor.executeQuery(QueryUtil.getJourneyQuery(queryHistoryInDays, queryFutureInDays));
                stopQueryProcessor.executeQuery(QueryUtil.getStopQuery());
                metroJourneyQueryProcessor.executeQuery(QueryUtil.getMetroJourneyQuery(queryHistoryInDays, queryFutureInDays));

                log.info("All data processed, thank you.");
            }
            catch (Exception e) {
                log.error("Exception during query ", e);
                shutdown();
            }
            finally {
                processingActive.set(false);
            }
        }
        else {
            log.warn("Processing already active, will not launch another task.");
        }
    }

    private void shutdown() {
        log.warn("Shutting down the application.");
        if (jedis != null) {
            jedis.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
        log.info("Shutdown completed, bye.");
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
