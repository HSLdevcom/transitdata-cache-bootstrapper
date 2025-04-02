package fi.hsl.transitdata.pubtransredisconnect;

import java.sql.ResultSet;

public abstract class AbstractResultSetProcessor {
    public RedisUtils redisUtils;
    public QueryUtils queryUtils;

    public AbstractResultSetProcessor(final RedisUtils redisUtils, final QueryUtils queryUtils) {
        this.redisUtils = redisUtils;
        this.queryUtils = queryUtils;
    }

    public abstract void processResultSet(final ResultSet resultSet) throws Exception;

    protected abstract String getQuery();
}
