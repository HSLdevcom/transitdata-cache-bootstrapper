package fi.hsl.transitdata.pubtransredisconnect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractQueryProcessor {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueryProcessor.class);
    
    public Connection connection;
    
    public AbstractQueryProcessor(final Connection connection) {
        this.connection = connection;
    }
    
    public abstract void executeAndProcessQuery(final AbstractResultSetProcessor processor);
    
    ResultSet executeQuery(final String query) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        return resultSet;
    }
    
    static void closeQuery(final ResultSet resultSet, long now) {
        Statement statement = null;
        try { statement = resultSet.getStatement(); } catch (Exception e) {
            log.error("Failed to get Statement", e);
        }
        if (resultSet != null)  try {
            resultSet.close();
            log.info("ResultSet closed. {}", now);
        } catch (Exception e) {
            log.error("Failed to close ResultSet", e);
        }
        if (statement != null)  try {
            statement.close();
            log.info("Statement closed. {}", now);
        } catch (Exception e) {
            log.error("Failed to close Statement", e);
        }
    }
}
