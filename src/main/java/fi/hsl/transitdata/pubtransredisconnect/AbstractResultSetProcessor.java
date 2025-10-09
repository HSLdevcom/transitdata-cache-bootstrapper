package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.redis.RedisStore;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.Collection;

public abstract class AbstractResultSetProcessor<T> {

    protected final RedisStore redisStore;
    protected final QueryUtils queryUtils;
    protected final Duration redisTtl;

    public AbstractResultSetProcessor(RedisStore redisStore, QueryUtils queryUtils, Duration redisTtl) {
        this.redisStore = redisStore;
        this.queryUtils = queryUtils;
        this.redisTtl = redisTtl;
    }

    abstract String getQuery();

    abstract Collection<T> collectResults(ResultSet resultSet) throws Exception;

    abstract void processItems(Collection<T> items) throws Exception;
}
