package fi.hsl.transitdata.pubtransredisconnect.processor;

import fi.hsl.transitdata.pubtransredisconnect.util.QueryUtils;
import fi.hsl.transitdata.pubtransredisconnect.util.RedisUtils;

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
