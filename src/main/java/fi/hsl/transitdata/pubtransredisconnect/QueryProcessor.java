package fi.hsl.transitdata.pubtransredisconnect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.slf4j.MDC.putCloseable;

public class QueryProcessor {

    private static final Logger log = LoggerFactory.getLogger(QueryProcessor.class);

    private final Connection connection;

    public QueryProcessor(final Connection connection) {
        this.connection = connection;
    }

    public <T> void executeAndProcessQuery(final AbstractResultSetProcessor<T> processor) {
        var startTime = System.currentTimeMillis();

        ResultSet resultSet = null;
        try (var ignored = putCloseable("processorName", processor.getClass().getName())) {
            log.info("Starting query");
            var query = processor.getQuery();
            log.info("Executing query");
            resultSet = connection.createStatement().executeQuery(query);
            log.info("Collecting results");
            var results = processor.collectResults(resultSet);
            log.info("Database query found {} rows", results.size());
            closeQuery(resultSet);
            log.info("Query closed");
            processor.processItems(results);
            log.info("Query processed");
        } catch (Exception e) {
            log.error("Failed to process query", e);
        } finally {
            closeQuery(resultSet);
        }

        long elapsed = (System.currentTimeMillis() - startTime) / 1000;
        log.info("Data handled in " + elapsed + " seconds");
    }

    public static void closeQuery(final ResultSet resultSet) {
        if (resultSet == null) {
            log.warn("ResultSet is null, nothing to close.");
            return;
        }
        try {
            if (resultSet.isClosed()) {
                log.info("ResultSet is already closed, nothing to close.");
                return;
            }
        } catch (SQLException e) {
            log.info("Error occured when trying to check if ResultSet is closed.");
        }
        Statement statement = null;
        try {
            statement = resultSet.getStatement();
        } catch (Exception e) {
            log.error("Failed to get Statement", e);
        }

        try {
            resultSet.close();
            log.info("ResultSet closed.");
        } catch (Exception e) {
            log.error("Failed to close ResultSet", e);
        }

        if (statement != null)
            try {
                statement.close();
                log.info("Statement closed.");
            } catch (Exception e) {
                log.error("Failed to close Statement", e);
            }
    }
}
