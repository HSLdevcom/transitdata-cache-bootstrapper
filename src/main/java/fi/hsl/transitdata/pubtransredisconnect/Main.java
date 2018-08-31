package fi.hsl.transitdata.pubtransredisconnect;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.io.File;
import java.sql.*;
import com.microsoft.sqlserver.jdbc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class Main {

	private static final Logger log = LoggerFactory.getLogger(Main.class);

	private static final String DVJ_ID = "dvj_id";
	private static final String DIRECTION = "direction";
	private static final String ROUTE_NAME = "route";
	private static final String START_TIME = "start_time";
	private static final String OPERATING_DAY = "operating_day";


	public static void main(String[] args) {

		String connectionString = "";

		final String redisHost = Optional.ofNullable(System.getenv("REDIS_HOST")).orElse("localhost");
		Jedis jedis = new Jedis(redisHost);

		try {
			connectionString = new Scanner(new File("/run/secrets/pubtrans_community_conn_string"))
					.useDelimiter("\\Z").next();
		} catch (Exception e) {
            log.error("Failed to read the DB connection string from the file", e);
		}

		if (connectionString.equals("")) {
			log.error("Connection string empty, aborting.");
			System.exit(1);
		}

		try (Connection connection = DriverManager.getConnection(connectionString)) {
			queryJourneyData(connection, jedis);
			queryStopData(connection, jedis);
		}
		catch (SQLException e) {
		    log.error("SQL Exception: ", e);
		}
	}

	private static void queryJourneyData(Connection connection, Jedis jedis) throws SQLException {

		Statement statement;
		ResultSet resultSet;

		String selectSql = new StringBuilder()
				.append("SELECT ")
				.append(" CONVERT(CHAR(16), DVJ.Id) AS " + DVJ_ID + ",")
				.append(" KVV.StringValue AS " + ROUTE_NAME + ",")
				.append(" SUBSTRING(")
				.append("    CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid),")
				.append("       12,")
				.append("       1")
				.append("   ) AS " + DIRECTION + ",")
				.append(" CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS " + OPERATING_DAY + ", ")
				.append(" RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2) ")
				.append(" + ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime))")
				.append("- ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS " + START_TIME + " ")
				.append(" FROM")
				.append("     ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ,")
				.append("     ptDOI4_Community.dbo.VehicleJourney AS VJ,")
				.append("     ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT,")
				.append("     ptDOI4_Community.T.KeyVariantValue AS KVV,")
				.append("     ptDOI4_Community.dbo.KeyType AS KT,")
				.append("     ptDOI4_Community.dbo.KeyVariantType AS KVT,")
				.append("     ptDOI4_Community.dbo.ObjectType AS OT")
				.append(" WHERE")
				.append("     DVJ.IsBasedOnVehicleJourneyId = VJ.Id")
				.append("     AND DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id")
				.append("     AND (")
				.append("         KT.Name = 'JoreIdentity'")
				.append("         OR KT.Name = 'JoreRouteIdentity'")
				.append("         OR KT.Name = 'RouteName'")
				.append("     )")
				.append("     AND KT.ExtendsObjectTypeNumber = OT.Number")
				.append("     AND OT.Name = 'VehicleJourney'")
				.append("     AND KT.Id = KVT.IsForKeyTypeId")
				.append("     AND KVT.Id = KVV.IsOfKeyVariantTypeId")
				.append("     AND KVV.IsForObjectId = VJ.Id")
				.append("     AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL")
				.append("     AND DVJ.OperatingDayDate >= '2018-06-06'")
				.append("     AND DVJ.OperatingDayDate < '2018-12-31'")
				.append("     AND DVJ.IsReplacedById IS NULL")
				.toString();

		log.info("Starting journey query");

		statement = connection.createStatement();
		resultSet = statement.executeQuery(selectSql);

		try {
			handleJourneyResultSet(resultSet, jedis);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		if (resultSet != null)  try { resultSet.close(); } catch (Exception e) {}
		if (statement != null)  try { statement.close(); } catch (Exception e) {}

	}


	private static void queryStopData(Connection connection, Jedis jedis) throws SQLException {

		Statement statement;
		ResultSet resultSet;

		String selectSql = new StringBuilder()
				.append("SELECT ")
				.append("[Gid], [Number] ")
				.append("FROM [ptDOI4_Community].[dbo].[JourneyPatternPoint] AS JPP ")
				.append("GROUP BY JPP.Gid, JPP.Number")
				.toString();

		log.info("Starting stop query");

		statement = connection.createStatement();
		resultSet = statement.executeQuery(selectSql);

		try {
			handleStopResultSet(resultSet, jedis);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		if (resultSet != null)  try { resultSet.close(); } catch (Exception e) {}
		if (statement != null)  try { statement.close(); } catch (Exception e) {}

	}

	private static void handleJourneyResultSet(ResultSet resultSet, Jedis jedis) throws Exception {

		int rowCounter = 0;

		while(resultSet.next()) {
		    Map<String, String> values = new HashMap<>();

		   	values.put("route-name", resultSet.getString(ROUTE_NAME));
			values.put("direction", resultSet.getString(DIRECTION));
			values.put("start-time", resultSet.getString(START_TIME));
			values.put("operating-day", resultSet.getString(OPERATING_DAY));

			jedis.hmset("dvj:" + resultSet.getString(DVJ_ID), values);

			rowCounter++;
		}

		log.info("Inserted " + rowCounter + " dvj keys");
	}

	private static void handleStopResultSet(ResultSet resultSet, Jedis jedis) throws Exception {

		int rowCounter = 0;

		while(resultSet.next()) {

			jedis.set("jpp:" + resultSet.getString(1), resultSet.getString(2));

			rowCounter++;
		}

		log.info("Inserted " + rowCounter + " jpp keys");
	}

}
