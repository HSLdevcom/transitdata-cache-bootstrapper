package fi.hsl.transitdata.pubtransredisconnect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;

public class QueryProcessor {

    private static final Logger log = LoggerFactory.getLogger(QueryProcessor.class);

    private Connection connection;
    private IResultSetProcessor processor;

    public QueryProcessor() {}

    public QueryProcessor(final Connection connection, final IResultSetProcessor processor) {
        this.connection = connection;
        this.processor = processor;
    }

    public QueryProcessor(final IResultSetProcessor processor) {
        this.processor = processor;
    }

    public void executeQuery(final String query) {
        log.info("Starting query");
        long now = System.currentTimeMillis();

        ResultSet resultSet = null;
        try {
            resultSet = QueryUtil.executeQuery(connection, query);
            processor.process(resultSet);
        } catch (Exception e) {
            log.error("Failed to process query", e);
        } finally {
            QueryUtil.closeQuery(resultSet);
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

    public IResultSetProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(final IResultSetProcessor processor) {
        this.processor = processor;
    }
}
