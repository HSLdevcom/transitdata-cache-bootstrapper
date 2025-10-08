package fi.hsl.transitdata.pubtransredisconnect;

import fi.hsl.common.redis.RedisStore;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

import static fi.hsl.transitdata.pubtransredisconnect.StopResultSetProcessor.StopResultItem;

public class StopResultSetProcessor extends AbstractResultSetProcessor<StopResultItem> {

    private static final Logger log = LoggerFactory.getLogger(StopResultSetProcessor.class);

    record StopResultItem(String gid, String number) {
    }

    public StopResultSetProcessor(RedisStore redisStore, QueryUtils queryUtils, Duration redisTtl) {
        super(redisStore, queryUtils, redisTtl);
    }

    @Override
    String getQuery() {
        return "SELECT [Gid], [Number] " +
                "FROM [ptDOI4_Community].[dbo].[JourneyPatternPoint] AS JPP " +
                "GROUP BY JPP.Gid, JPP.Number ";
    }

    @Override
    Collection<StopResultItem> collectResults(ResultSet resultSet) throws Exception {
        final var items = new ArrayList<StopResultItem>();

        while (resultSet.next()) {
            items.add(new StopResultItem(
                    resultSet.getString("Gid"),
                    resultSet.getString("Number")
            ));
        }

        return items;
    }

    @Override
    void processItems(Collection<StopResultItem> items) throws Exception {
        int rowCounter = 0;
        int redisCounter = 0;

        for (var item : items) {
            rowCounter++;

            var key = TransitdataProperties.REDIS_PREFIX_JPP + item.gid;
            var response = redisStore.setValue(key, item.number);
            if (redisStore.checkResponse(response)) {
                redisCounter++;
            } else {
                log.error("Failed to set stop key {}, Redis returned {}", key, response);
            }
        }

        log.info("Inserted {} redis stop id keys (jpp-id) for {} DB rows", redisCounter, rowCounter);
    }
}
