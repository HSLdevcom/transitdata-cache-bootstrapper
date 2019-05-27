package fi.hsl.transitdata.pubtransredisconnect;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.io.File;
import java.sql.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.typesafe.config.*;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    Config config;

    private final String connectionString;

    private ScheduledExecutorService executor;
    private AtomicBoolean processingActive = new AtomicBoolean(false);

    private RedisUtils redisUtils;
    private String from;
    private String to;

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
        redisUtils = new RedisUtils(config);
        final int queryHistoryInDays = config.getInt("bootstrapper.queryHistoryInDays");
        final int queryFutureInDays = config.getInt("bootstrapper.queryFutureInDays");
        log.info("Fetching data from -" + queryHistoryInDays + " days to +" + queryFutureInDays + " days");
        from = formatDate(-queryHistoryInDays);
        to = formatDate(queryFutureInDays);
    }

    public static String formatDate(int offsetInDays) {
        LocalDate now = LocalDate.now();
        LocalDate then = now.plus(offsetInDays, ChronoUnit.DAYS);

        String formattedString = DateTimeFormatter.ISO_LOCAL_DATE.format(then);
        log.debug("offsetInDays results to date " + formattedString);

        return formattedString;
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
                final QueryProcessor queryProcessor = new QueryProcessor(connection);
                final JourneyResultSetProcessor journeyResultSetProcessor = new JourneyResultSetProcessor(redisUtils, from, to);
                final StopResultSetProcessor stopResultSetProcessor = new StopResultSetProcessor(redisUtils);
                final MetroJourneyResultSetProcessor metroJourneyResultSetProcessor = new MetroJourneyResultSetProcessor(redisUtils, from, to);

                queryProcessor.executeAndProcessQuery(journeyResultSetProcessor);
                queryProcessor.executeAndProcessQuery(stopResultSetProcessor);
                queryProcessor.executeAndProcessQuery(metroJourneyResultSetProcessor);

                redisUtils.updateTimestamp();

                log.info("All data processed, thank you.");
            }
            catch (SQLServerException sqlServerException) {
                String msg = "SQLServerException during query, Driver Error code: "
                        + sqlServerException.getErrorCode()
                        + " and SQL State: " + sqlServerException.getSQLState();
                log.error(msg, sqlServerException);
                shutdown();
            }
            catch (Exception e) {
                log.error("Unknown exception during query ", e);
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
        if (redisUtils.jedis != null) {
            redisUtils.jedis.close();
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
