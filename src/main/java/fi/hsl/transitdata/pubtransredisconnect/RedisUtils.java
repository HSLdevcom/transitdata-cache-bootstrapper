package fi.hsl.transitdata.pubtransredisconnect;

import com.typesafe.config.Config;
import fi.hsl.common.pulsar.PulsarApplicationContext;
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

    public RedisUtils(final PulsarApplicationContext context) {
        final Config config = context.getConfig();
        jedis = context.getJedis();
        redisTTLInSeconds = config.getInt("bootstrapper.redisTTLInDays") * 24 * 60 * 60;
        log.info("Redis TTL in secs: " + redisTTLInSeconds);
    }

    public String setValue(final String key, final String value) {
        synchronized (jedis) {
            return jedis.setex(key, redisTTLInSeconds, value);
        }
    }

    public String setValues(final String key, final Map<String, String> values) {
        synchronized (jedis) {
            return jedis.hmset(key, values);
        }
    }

    public Long setExpire(final String key) {
        synchronized (jedis) {
            return jedis.expire(key, redisTTLInSeconds);
        }
    }

    public void updateTimestamp() {
        synchronized (jedis) {
            final OffsetDateTime now = OffsetDateTime.now();
            final String ts = DateTimeFormatter.ISO_INSTANT.format(now);
            log.info("Updating Redis with latest timestamp: " + ts);
            final String result = jedis.set(TransitdataProperties.KEY_LAST_CACHE_UPDATE_TIMESTAMP, ts);
            if (!checkResponse(result)) {
                log.error("Failed to update cache timestamp to Redis!");
            }
        }
    }

    public boolean checkResponse(final String response) {
        return response != null && response.equalsIgnoreCase("OK");
    }
}
