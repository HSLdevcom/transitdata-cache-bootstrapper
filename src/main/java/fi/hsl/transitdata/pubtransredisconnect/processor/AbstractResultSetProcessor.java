package fi.hsl.transitdata.pubtransredisconnect.processor;

import fi.hsl.transitdata.pubtransredisconnect.model.DatabaseQueryResult;
import fi.hsl.transitdata.pubtransredisconnect.util.QueryUtils;
import fi.hsl.transitdata.pubtransredisconnect.util.RedisUtils;

import java.sql.ResultSet;
import java.util.List;

public abstract class AbstractResultSetProcessor {
    public RedisUtils redisUtils;
    public QueryUtils queryUtils;

    public AbstractResultSetProcessor(final RedisUtils redisUtils, final QueryUtils queryUtils) {
        this.redisUtils = redisUtils;
        this.queryUtils = queryUtils;
    }

    public abstract List<DatabaseQueryResult> processResultSet(final ResultSet resultSet) throws Exception;
    
    public abstract void saveToRedis(List<DatabaseQueryResult> results);
    
    protected abstract String getQuery();
}
