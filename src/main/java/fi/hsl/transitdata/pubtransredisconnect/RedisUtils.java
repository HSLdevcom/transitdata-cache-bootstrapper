package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static fi.hsl.common.transitdata.TransitdataProperties.KEY_LAST_CACHE_UPDATE_TIMESTAMP;

public class RedisUtils {
    private static final Logger log = LoggerFactory.getLogger(RedisUtils.class);

    private final JedisExecutor jedisExecutor;
    private final int redisTTLInSeconds;

    public RedisUtils(PulsarApplicationContext context, JedisExecutor jedisExecutor) {
        this.jedisExecutor = jedisExecutor;
        this.redisTTLInSeconds = (int) Duration.ofDays(context.getConfig().getInt("bootstrapper.redisTTLInDays")).toSeconds();
    }

    public String setValue(String key, String value) {
        return jedisExecutor.execute(jedis -> jedis.setex(key, redisTTLInSeconds, value));
    }

    public String setValues(String key, Map<String, String> values) {
        return jedisExecutor.execute(jedis -> jedis.hmset(key, values));
    }

    public void setExpire(String key) {
        jedisExecutor.execute(jedis -> jedis.expire(key, redisTTLInSeconds));
    }

    public void updateTimestamp() {
        jedisExecutor.execute(jedis -> {
            final OffsetDateTime now = OffsetDateTime.now();
            final String ts = DateTimeFormatter.ISO_INSTANT.format(now);
            log.info("Updating Redis with latest timestamp: " + ts);
            final var result = jedis.set(KEY_LAST_CACHE_UPDATE_TIMESTAMP, ts);
            if (!checkResponse(result)) {
                log.error("Failed to update cache timestamp to Redis!");
            }
            return result;
        });
    }

    public boolean checkResponse(final String response) {
        return response != null && response.equalsIgnoreCase("OK");
    }
}
