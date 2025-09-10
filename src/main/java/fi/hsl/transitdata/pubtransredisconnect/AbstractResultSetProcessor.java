package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.redis.RedisStore;

import java.sql.ResultSet;
import java.time.Duration;

public abstract class AbstractResultSetProcessor {

    protected final RedisStore redisStore;
    protected final QueryUtils queryUtils;
    protected final Duration redisTtl;

    public AbstractResultSetProcessor(RedisStore redisStore, QueryUtils queryUtils, Duration redisTtl) {
        this.redisStore = redisStore;
        this.queryUtils = queryUtils;
        this.redisTtl = redisTtl;
    }

    public abstract void processResultSet(final ResultSet resultSet) throws Exception;

    protected abstract String getQuery();
}
