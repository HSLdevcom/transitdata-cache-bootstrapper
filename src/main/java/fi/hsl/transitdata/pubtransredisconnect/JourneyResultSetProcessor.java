package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class JourneyResultSetProcessor implements IResultSetProcessor {

    private static final Logger log = LoggerFactory.getLogger(JourneyResultSetProcessor.class);

    private Jedis jedis;
    private int redisTTLInSeconds;

    public JourneyResultSetProcessor(final Jedis jedis, final int redisTTLInSeconds) {
        this.jedis = jedis;
        this.redisTTLInSeconds = redisTTLInSeconds;
    }

    public void process(final ResultSet resultSet) throws Exception {
        int rowCounter = 0;

        while(resultSet.next()) {
            Map<String, String> values = new HashMap<>();
            values.put(TransitdataProperties.KEY_ROUTE_NAME, resultSet.getString(QueryUtil.ROUTE_NAME));
            values.put(TransitdataProperties.KEY_DIRECTION, resultSet.getString(QueryUtil.DIRECTION));
            values.put(TransitdataProperties.KEY_START_TIME, resultSet.getString(QueryUtil.START_TIME));
            values.put(TransitdataProperties.KEY_OPERATING_DAY, resultSet.getString(QueryUtil.OPERATING_DAY));

            String key = TransitdataProperties.REDIS_PREFIX_DVJ + resultSet.getString(QueryUtil.DVJ_ID);
            jedis.hmset(key, values);
            jedis.expire(key, redisTTLInSeconds);

            //Insert a composite key that allows reverse lookup of the dvj id
            //The format is route-direction-date-time
            String joreKey = TransitdataProperties.formatJoreId(resultSet.getString(QueryUtil.ROUTE_NAME),
                    resultSet.getString(QueryUtil.DIRECTION), resultSet.getString(QueryUtil.OPERATING_DAY),
                    resultSet.getString(QueryUtil.START_TIME));
            jedis.set(joreKey, resultSet.getString(QueryUtil.DVJ_ID));
            jedis.expire(joreKey, redisTTLInSeconds);

            rowCounter++;
        }

        log.info("Inserted " + rowCounter + " dvj keys");
    }
}
