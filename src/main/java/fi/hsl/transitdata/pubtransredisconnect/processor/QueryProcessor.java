package fi.hsl.transitdata.pubtransredisconnect.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryProcessor {

    private static final Logger log = LoggerFactory.getLogger(QueryProcessor.class);

    public Connection connection;

    public QueryProcessor(final Connection connection) {
        this.connection = connection;
    }

    public void executeAndProcessQuery(final AbstractResultSetProcessor processor) {
        final String processorName = processor.getClass().getName();
        long now = System.currentTimeMillis();
        log.info("Starting query with result set processor {}. {}", processorName, now);

        ResultSet resultSet = null;
        final String query = processor.getQuery();
        log.info("Executing query... {}", now);
        
        try {
            resultSet = executeQuery(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuery(resultSet, now);
            log.info("Query closed... {}", now);
        }
        
        log.info("Processing result set... {}", now);
        try {
            processor.processResultSet(resultSet);
        } catch (JedisConnectionException e) {
            log.error(String.format("Failed to connect to Redis while running processor %s.", processorName), e);
            throw e;
        } catch (Exception e) {
            log.error("Failed to process result set", e);
        }
        log.info("Query processed. {}", now);
        
        long elapsed = (System.currentTimeMillis() - now) / 1000;
        log.info("Data handled in " + elapsed + " seconds");
    }

    private ResultSet executeQuery(final String query) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        return resultSet;
    }

    private static void closeQuery(final ResultSet resultSet, long now) {
        Statement statement = null;
        try { statement = resultSet.getStatement(); } catch (Exception e) {
            log.error("Failed to get Statement", e);
        }
        if (resultSet != null)  try {
            resultSet.close();
            log.info("ResultSet closed. {}", now);
        } catch (Exception e) {
            log.error("Failed to close ResultSet", e);
        }
        if (statement != null)  try {
            statement.close();
            log.info("Statement closed. {}", now);
        } catch (Exception e) {
            log.error("Failed to close Statement", e);
        }
    }
}
