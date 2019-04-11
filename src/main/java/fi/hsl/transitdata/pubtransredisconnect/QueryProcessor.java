package fi.hsl.transitdata.pubtransredisconnect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryProcessor {

    private static final Logger log = LoggerFactory.getLogger(QueryProcessor.class);

    private Connection connection;

    public QueryProcessor() {}

    public QueryProcessor(final Connection connection) {
        this.connection = connection;
    }

    public void executeAndProcessQuery(final AbstractResultSetProcessor processor) {
        log.info("Starting query with result set processor {}.", processor.getClass().getName());
        long now = System.currentTimeMillis();

        ResultSet resultSet = null;
        try {
            final String query = processor.getQuery();
            resultSet = executeQuery(query);
            processor.processResultSet(resultSet);
        } catch (Exception e) {
            log.error("Failed to process query", e);
        } finally {
            closeQuery(resultSet);
        }

        long elapsed = (System.currentTimeMillis() - now) / 1000;
        log.info("Data handled in " + elapsed + " seconds");
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(final Connection connection) {
        this.connection = connection;
    }

    private ResultSet executeQuery(final String query) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        return resultSet;
    }

    private static void closeQuery(final ResultSet resultSet) {
        Statement statement = null;
        try { statement = resultSet.getStatement(); } catch (Exception e) {
            log.error("Failed to get Statement", e);
        }
        if (resultSet != null)  try { resultSet.close(); } catch (Exception e) {
            log.error("Failed to close ResultSet", e);
        }
        if (statement != null)  try { statement.close(); } catch (Exception e) {
            log.error("Failed to close Statement", e);
        }
    }
}
