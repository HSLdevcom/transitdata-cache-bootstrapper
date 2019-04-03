package fi.hsl.transitdata.pubtransredisconnect;

import java.sql.ResultSet;

public interface IResultSetProcessor  {
    void process(final ResultSet resultSet) throws Exception;
}
