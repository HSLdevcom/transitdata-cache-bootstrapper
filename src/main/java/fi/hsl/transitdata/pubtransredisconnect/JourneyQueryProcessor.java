package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JourneyQueryProcessor extends AbstractQueryProcessor {
    public RedisUtils redisUtils;
    public QueryUtils queryUtils;

    private static final Logger log = LoggerFactory.getLogger(JourneyQueryProcessor.class);

    public JourneyQueryProcessor(final Connection connection, final RedisUtils redisUtils, QueryUtils queryUtils) {
        super(connection);
        this.redisUtils = redisUtils;
        this.queryUtils = queryUtils;
    }

    public void executeAndProcessQuery(final AbstractResultSetProcessor resultSetProcessor) {
        final String resultSetProcessorName = resultSetProcessor.getClass().getName();
        long now = System.currentTimeMillis();
        List<Journey> journeys = null;
        log.info("Starting query with result set processor {}. {}", resultSetProcessorName, now);

        ResultSet resultSet = null;
        try {
            final String query = resultSetProcessor.getQuery();
            log.info("Executing query... {}", now);
            resultSet = executeQuery(query);
            log.info("Transforming result set to list of journeys... {}", now);
            journeys = resultSetToJourneyList(resultSet);
            log.info("Query processed. {}", now);
        } catch (JedisConnectionException e) {
            log.error(String.format("Failed to connect to Redis while running processor %s.", resultSetProcessorName), e);
            throw e;
        } catch (Exception e) {
            log.error("Failed to process query", e);
        } finally {
            closeQuery(resultSet, now);
        }
        
        log.info("Saving data to Redis... {}", now);
        saveToRedis(journeys);
        
        long elapsed = (System.currentTimeMillis() - now) / 1000;
        log.info("Data handled in " + elapsed + " seconds");
    }
    
    private void saveToRedis(List<Journey> journeys) {
        int tripInfoCounter = 0;
        int lookupCounter = 0;
        for (Journey journey : journeys) {
            String key = TransitdataProperties.REDIS_PREFIX_DVJ + journey.toMap().get(TransitdataProperties.KEY_DVJ_ID);
            String response = redisUtils.setValues(key, journey.toMap());
            if (redisUtils.checkResponse(response)) {
                redisUtils.setExpire(key);
                tripInfoCounter++;
                
                //Insert a composite key that allows reverse lookup of the dvj id
                //The format is route-direction-date-time
                final String joreKey = TransitdataProperties.formatJoreId(journey.routeName,
                        journey.direction, journey.operatingDay, journey.startTime);
                response = redisUtils.setValue(joreKey, journey.dvjId);
                if (redisUtils.checkResponse(response)) {
                    redisUtils.setExpire(joreKey);
                    lookupCounter++;
                } else {
                    log.error("Failed to set reverse-lookup key {}, Redis returned {}", joreKey, response);
                }
            } else {
                log.error("Failed to set journey key {}, Redis returned {}", key, response);
            }
        }
        log.info("Inserted {} trip info and {} reverse-lookup keys", tripInfoCounter, lookupCounter);
    }
    
    private List<Journey> resultSetToJourneyList(ResultSet resultSet) throws SQLException {
        int rowCounter = 0;
        List<Journey> journeys = new ArrayList<>();
        
        while (resultSet.next()) {
            rowCounter++;
            String routeName = resultSet.getString(queryUtils.ROUTE_NAME);
            String direction = resultSet.getString(queryUtils.DIRECTION);
            String startTime = resultSet.getString(queryUtils.START_TIME);
            String operatingDay = resultSet.getString(queryUtils.OPERATING_DAY);
            String dvjId = resultSet.getString(queryUtils.DVJ_ID);
            
            Journey journey = new Journey(routeName, direction, startTime, operatingDay, dvjId);
            journeys.add(journey);
        }
        
        log.info("Processed {} rows from the result set", rowCounter);
        return journeys;
    }
    
    private class Journey {
        private String routeName;
        private String direction;
        private String startTime;
        private String operatingDay;
        private String dvjId;

        public Journey(String routeName, String direction, String startTime, String operatingDay, String dvjId) {
            this.routeName = routeName;
            this.direction = direction;
            this.startTime = startTime;
            this.operatingDay = operatingDay;
            this.dvjId = dvjId;
        }

        public Map<String, String> toMap() {
            Map<String, String> map = new HashMap<>();
            map.put(TransitdataProperties.KEY_ROUTE_NAME, routeName);
            map.put(TransitdataProperties.KEY_DIRECTION, direction);
            map.put(TransitdataProperties.KEY_START_TIME, startTime);
            map.put(TransitdataProperties.KEY_OPERATING_DAY, operatingDay);
            return map;
        }
    }
}
