package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class StopResultSetProcessor implements IResultSetProcessor {

    private static final Logger log = LoggerFactory.getLogger(StopResultSetProcessor.class);

    private Jedis jedis;
    private int redisTTLInSeconds;

    public StopResultSetProcessor(final Jedis jedis, final int redisTTLInSeconds) {
        this.jedis = jedis;
        this.redisTTLInSeconds = redisTTLInSeconds;
    }

    public void process(final ResultSet resultSet) throws Exception {
        int rowCounter = 0;

        while(resultSet.next()) {
            String key = TransitdataProperties.REDIS_PREFIX_JPP  + resultSet.getString(1);
            jedis.setex(key, redisTTLInSeconds, resultSet.getString(2));

            rowCounter++;
        }

        updateTimestamp(jedis);

        log.info("Inserted " + rowCounter + " jpp keys");
    }

    void updateTimestamp(Jedis jedis) {
        OffsetDateTime now = OffsetDateTime.now();
        String ts = DateTimeFormatter.ISO_INSTANT.format(now);
        log.info("Updating Redis with latest timestamp: " + ts);
        jedis.set(TransitdataProperties.KEY_LAST_CACHE_UPDATE_TIMESTAMP, ts);
    }
}
