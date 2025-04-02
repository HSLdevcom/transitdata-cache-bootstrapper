package fi.hsl.transitdata.pubtransredisconnect.processor;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.transitdata.pubtransredisconnect.model.StopResult;
import fi.hsl.transitdata.pubtransredisconnect.util.QueryUtils;
import fi.hsl.transitdata.pubtransredisconnect.util.RedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.ArrayList;

public class StopResultSetProcessor extends AbstractResultSetProcessor {

    private static final Logger log = LoggerFactory.getLogger(StopResultSetProcessor.class);

    public StopResultSetProcessor(final RedisUtils redisUtils, final QueryUtils queryUtils) {
        super(redisUtils, queryUtils);
    }

    public void processResultSet(final ResultSet resultSet) throws Exception {
        ArrayList<StopResult> results = new ArrayList<>();

        while(resultSet.next()) {
            results.add(new StopResult(
                    resultSet.getString("Gid"),
                    resultSet.getString("Number")
            ));
        }

        saveToRedis(results);
    }

    private void saveToRedis(ArrayList<StopResult> results) {
        int redisCounter = 0;
        for (StopResult result : results) {
            String key = TransitdataProperties.REDIS_PREFIX_JPP  + result.getGid();
            String response = redisUtils.setValue(key, result.getNumber());
            if (redisUtils.checkResponse(response)) {
                redisCounter++;
            } else {
                log.error("Failed to set stop key {}, Redis returned {}", key, response);
            }
        }

        log.info("Inserted {} redis stop id keys (jpp-id) for {} DB rows", redisCounter, results.size());
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
