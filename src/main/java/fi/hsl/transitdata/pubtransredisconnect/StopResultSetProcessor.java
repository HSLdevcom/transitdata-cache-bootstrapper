package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

public class StopResultSetProcessor extends AbstractResultSetProcessor {

    private static final Logger log = LoggerFactory.getLogger(StopResultSetProcessor.class);

    public StopResultSetProcessor(final RedisUtils redisUtils) {
        super(redisUtils);
    }

    public void processResultSet(final ResultSet resultSet) throws Exception {
        int rowCounter = 0;
        int redisCounter = 0;

        while(resultSet.next()) {
            rowCounter++;
            String key = TransitdataProperties.REDIS_PREFIX_JPP  + resultSet.getString("Gid");
            String response = redisUtils.setValue(key, resultSet.getString("Number"));
            if (redisUtils.checkResponse(response)) {
                redisCounter++;
            } else {
                log.error("Failed to set stop key {}, Redis returned {}", key, response);
            }
        }

        log.info("Inserted {} redis stop id keys (jpp-id) for {} DB rows", redisCounter, rowCounter);
    }

    protected String getQuery() {
        String query = new StringBuilder()
                .append("SELECT ")
                .append("[Gid], [Number] ")
                .append("FROM [ptDOI4_Community].[dbo].[JourneyPatternPoint] AS JPP ")
                .append("GROUP BY JPP.Gid, JPP.Number ")
                .toString();
        return query;
    }
}
