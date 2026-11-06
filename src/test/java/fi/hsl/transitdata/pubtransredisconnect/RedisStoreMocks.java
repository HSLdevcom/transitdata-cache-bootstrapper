package fi.hsl.transitdata.pubtransredisconnect;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import fi.hsl.common.redis.RedisStore;

/** A mocked {@link RedisStore} whose writes succeed and whose response checks are the real ones. */
final class RedisStoreMocks {

    private RedisStoreMocks() {
    }

    static RedisStore succeedingRedisStore() {
        RedisStore redisStore = mock(RedisStore.class);
        doCallRealMethod().when(redisStore).checkResponse(nullable(String.class));
        doCallRealMethod().when(redisStore).checkResponse(nullable(Long.class));
        when(redisStore.setValue(anyString(), nullable(String.class))).thenReturn("OK");
        when(redisStore.setValues(anyString(), any())).thenReturn("OK");
        when(redisStore.setExpire(anyString(), any())).thenReturn(1L);
        return redisStore;
    }
}
