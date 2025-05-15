package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JourneyQueryProcessor extends AbstractQueryProcessor {
    public QueryUtils queryUtils;

    private static final Logger log = LoggerFactory.getLogger(JourneyQueryProcessor.class);

    public JourneyQueryProcessor(final Connection connection, QueryUtils queryUtils) {
        super(connection);
        this.queryUtils = queryUtils;
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
            log.info("Transforming result set to value map... {}", now);
            final Map<String, String> values = resultSetToValueMap(resultSet);
            //log.info("Processing result set... {}", now);
            //processor.processResultSet(resultSet);
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
    
    private Map<String, String> resultSetToValueMap(ResultSet resultSet) throws SQLException {
        final Map<String, String> values = new HashMap<>();
        values.put(TransitdataProperties.KEY_ROUTE_NAME, resultSet.getString(queryUtils.ROUTE_NAME));
        values.put(TransitdataProperties.KEY_DIRECTION, resultSet.getString(queryUtils.DIRECTION));
        values.put(TransitdataProperties.KEY_START_TIME, resultSet.getString(queryUtils.START_TIME));
        values.put(TransitdataProperties.KEY_OPERATING_DAY, resultSet.getString(queryUtils.OPERATING_DAY));
        return values;
    }
}
