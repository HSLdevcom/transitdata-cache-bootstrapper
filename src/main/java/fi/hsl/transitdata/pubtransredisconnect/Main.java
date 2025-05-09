package fi.hsl.transitdata.pubtransredisconnect;

import java.sql.*;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.typesafe.config.*;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    }

    public void start() {
        initialize();
        process();
    }

    private void initialize() {
        redisUtils = new RedisUtils(context);
        final int queryHistoryInDays = config.getInt("bootstrapper.queryHistoryInDays");
        final int queryFutureInDays = config.getInt("bootstrapper.queryFutureInDays");
        final int queryMinutesFromEvenHour = config.getInt("bootstrapper.queryMinutesFromEvenHour");
        log.info("Fetching data from -{} days to +{} days. {} minutes from even hour.",
                queryHistoryInDays, queryFutureInDays, queryMinutesFromEvenHour);
        queryUtils = new QueryUtils(queryHistoryInDays, queryFutureInDays);
    }
    
    private void process() {
        log.info("Fetching data");
        try (Connection connection = DriverManager.getConnection(connectionString)) {
            final QueryProcessor queryProcessor = new QueryProcessor(connection);
            final JourneyResultSetProcessor journeyResultSetProcessor = new JourneyResultSetProcessor(redisUtils, queryUtils);
            final StopResultSetProcessor stopResultSetProcessor = new StopResultSetProcessor(redisUtils, queryUtils);
            final MetroJourneyResultSetProcessor metroJourneyResultSetProcessor = new MetroJourneyResultSetProcessor(redisUtils, queryUtils);
            
            queryProcessor.executeAndProcessQuery(journeyResultSetProcessor);
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
        } finally {
            log.warn("Shutting down the application.");
            if (redisUtils.jedis != null) {
                redisUtils.jedis.close();
            }
            log.info("Shutdown completed, bye.");
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
        } catch (Exception e) {
            log.error("Exception at main", e);
            System.exit(1);
        }
        
        System.exit(0); // Exit with success code after successful execution
    }
}
