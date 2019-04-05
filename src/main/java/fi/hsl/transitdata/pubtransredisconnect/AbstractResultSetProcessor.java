package fi.hsl.transitdata.pubtransredisconnect;

import java.sql.ResultSet;

public abstract class AbstractResultSetProcessor {
    public RedisUtils redisUtils;

    public AbstractResultSetProcessor(final RedisUtils redisUtils) {
        this.redisUtils = redisUtils;
    }

    public abstract void processResultSet(final ResultSet resultSet) throws Exception;

    protected abstract String getQuery();
}
