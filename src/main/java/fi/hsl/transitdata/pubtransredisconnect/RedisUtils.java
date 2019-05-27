package fi.hsl.transitdata.pubtransredisconnect;

import com.typesafe.config.Config;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class RedisUtils {
    private static final Logger log = LoggerFactory.getLogger(RedisUtils.class);

    public Jedis jedis;
    public int redisTTLInSeconds;

    public RedisUtils(final Config config) {
        final String redisHost = config.getString("redis.host");
        log.info("Connecting to redis at " + redisHost);
        jedis = new Jedis(redisHost);

        redisTTLInSeconds = config.getInt("bootstrapper.redisTTLInDays") * 24 * 60 * 60;
        log.info("Redis TTL in secs: " + redisTTLInSeconds);
    }

    public String setValue(final String key, final String value) {
        return jedis.setex(key, redisTTLInSeconds, value);
    }

    public String setValues(final String key, final Map<String, String> values) {
        return jedis.hmset(key, values);
    }

    public Long setExpire(final String key) {
        return jedis.expire(key, redisTTLInSeconds);
    }

    public void updateTimestamp() {
        final OffsetDateTime now = OffsetDateTime.now();
        final String ts = DateTimeFormatter.ISO_INSTANT.format(now);
        log.info("Updating Redis with latest timestamp: " + ts);
        final String result = jedis.set(TransitdataProperties.KEY_LAST_CACHE_UPDATE_TIMESTAMP, ts);
        if (!checkResponse(result)) {
            log.error("Failed to update cache timestamp to Redis!");
        }
    }

    public boolean checkResponse(final String response) {
        return response != null && response.equalsIgnoreCase("OK");
    }
}
