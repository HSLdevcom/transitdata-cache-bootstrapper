package fi.hsl.transitdata.pubtransredisconnect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.sql.Connection;
import java.sql.ResultSet;

public class QueryProcessor extends AbstractQueryProcessor {

    private static final Logger log = LoggerFactory.getLogger(QueryProcessor.class);

    public QueryProcessor(final Connection connection) {
        super(connection);
    }

    public void executeAndProcessQuery(final AbstractResultSetProcessor processor) {
        final String processorName = processor.getClass().getName();
        long now = System.currentTimeMillis();
        log.info("Starting query with result set processor {}. {}", processorName, now);

        ResultSet resultSet = null;
        try {
            final String query = processor.getQuery();
            log.info("Executing query... {}", now);
            resultSet = executeQuery(query);
            log.info("Processing result set... {}", now);
            processor.processResultSet(resultSet);
            log.info("Query processed. {}", now);
        } catch (JedisConnectionException e) {
            log.error(String.format("Failed to connect to Redis while running processor %s.", processorName), e);
            throw e;
        } catch (Exception e) {
            log.error("Failed to process query", e);
        } finally {
            closeQuery(resultSet, now);
        }

        long elapsed = (System.currentTimeMillis() - now) / 1000;
        log.info("Data handled in " + elapsed + " seconds");
    }
}
