package fi.hsl.transitdata.pubtransredisconnect;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class QueryProcessorTest {
    private static final String DB_PASSWORD = "Test4321";

    @Rule
    public GenericContainer mssql = new GenericContainer(DockerImageName.parse("mcr.microsoft.com/mssql/server:2017-latest"))
            .withEnv("ACCEPT_EULA", "Y")
            .withEnv("MSSQL_SA_PASSWORD", DB_PASSWORD)
            .withEnv("MSSQL_PID", "Developer")
            .withExposedPorts(1433);

    private QueryProcessor queryProcessor;

    @Before
    public void setup() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:sqlserver://" + mssql.getHost() + ":" + mssql.getFirstMappedPort() + ";user=sa;password=" + DB_PASSWORD + ";");

        connection.prepareStatement("CREATE TABLE test (id INTEGER, val TEXT)").execute();
        connection.prepareStatement("INSERT INTO test (id, val) VALUES (1, 'a'), (2, 'b')").execute();

        queryProcessor = new QueryProcessor(connection);
    }

    @Test
    public void testQueryProcessor() {
        final AtomicInteger rows = new AtomicInteger(0);

        AbstractResultSetProcessor resultSetProcessor = new AbstractResultSetProcessor(null, null) {
            @Override
            public void processResultSet(ResultSet resultSet) throws Exception {
                while (resultSet.next()) {
                    rows.incrementAndGet();
                }
            }

            @Override
            protected String getQuery() {
                return "SELECT * FROM test;";
            }
        };

        queryProcessor.executeAndProcessQuery(resultSetProcessor);

        assertEquals(2, rows.get());
    }
}
