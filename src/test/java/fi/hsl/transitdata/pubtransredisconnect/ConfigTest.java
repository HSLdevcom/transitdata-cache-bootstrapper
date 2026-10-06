package fi.hsl.transitdata.pubtransredisconnect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import org.junit.Test;

/** The configuration the job runs with when no environment variables override environment.conf. */
public class ConfigTest {

    private final Config config = ConfigParser.createConfig();

    @Test
    public void bootstrapperDefaults() {
        assertEquals(3, config.getInt("bootstrapper.redisTTLInDays"));
        assertEquals(3, config.getInt("bootstrapper.queryHistoryInDays"));
        assertEquals(90, config.getInt("bootstrapper.queryFutureInDays"));
    }

    @Test
    public void pulsarConsumerAndProducerAreDisabled() {
        assertFalse(config.getBoolean("pulsar.consumer.enabled"));
        assertFalse(config.getBoolean("pulsar.producer.enabled"));
        assertFalse(config.getBoolean("pulsar.admin.enabled"));
    }

    @Test
    public void redisSentinelIsEnabledWithHealthCheck() {
        assertTrue(config.getBoolean("redisCluster.enabled"));
        assertTrue(config.getBoolean("redisCluster.healthCheck"));
        assertEquals("mymaster", config.getString("redisCluster.masterName"));
    }

    @Test
    public void healthServerIsOffByDefault() {
        assertFalse(config.getBoolean("health.enabled"));
        assertEquals(8090, config.getInt("health.port"));
    }
}
