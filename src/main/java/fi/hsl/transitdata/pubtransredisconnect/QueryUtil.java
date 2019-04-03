package fi.hsl.transitdata.pubtransredisconnect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class QueryUtil {

    private static final Logger log = LoggerFactory.getLogger(QueryUtil.class);

    public static final String DVJ_ID = "dvj_id";
    public static final String DIRECTION = "direction";
    public static final String ROUTE_NAME = "route";
    public static final String START_TIME = "start_time";
    public static final String OPERATING_DAY = "operating_day";
    public static final String STOP_NUMBER = "stop_number";

    private static QueryUtil ourInstance = new QueryUtil();

    public static QueryUtil getInstance() {
        return ourInstance;
    }

    private QueryUtil() {}

    public static ResultSet executeQuery(final Connection connection, final String query) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        return resultSet;
    }

    public static void closeQuery(final ResultSet resultSet) {
        Statement statement = null;
        try { statement = resultSet.getStatement(); } catch (Exception e) {
            log.error("Failed to get Statement", e);
        }
        if (resultSet != null)  try { resultSet.close(); } catch (Exception e) {
            log.error("Failed to close ResultSet", e);
        }
        if (statement != null)  try { statement.close(); } catch (Exception e) {
            log.error("Failed to close Statement", e);
        }
    }

    public static String getJourneyQuery(final int queryHistoryInDays, final int queryFutureInDays) {
        final String from = formatDate(-queryHistoryInDays);
        final String to = formatDate(queryFutureInDays);
        String query = new StringBuilder()
                .append("SELECT ")
                .append("   CONVERT(CHAR(16), DVJ.Id) AS " + DVJ_ID + ", ")
                .append("   KVV.StringValue AS " + ROUTE_NAME + ", ")
                .append("   SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1) AS " + DIRECTION + ", ")
                .append("   CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS " + OPERATING_DAY + ", ")
                .append("   RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2) ")
                .append("       + ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime)) ")
                .append("       - ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS " + START_TIME + " ")
                .append("FROM ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ ")
                .append("LEFT JOIN ptDOI4_Community.dbo.VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id) ")
                .append("LEFT JOIN ptDOI4_Community.T.KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number) ")
                .append("WHERE ")
                .append("   ( ")
                .append("       KT.Name = 'JoreIdentity' ")
                .append("       OR KT.Name = 'JoreRouteIdentity' ")
                .append("       OR KT.Name = 'RouteName' ")
                .append("   ) ")
                .append("   AND OT.Name = 'VehicleJourney' ")
                .append("   AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL ")
                .append("   AND DVJ.OperatingDayDate >= '" + from + "' ")
                .append("   AND DVJ.OperatingDayDate < '" + to + "' ")
                .append("   AND DVJ.IsReplacedById IS NULL ")
                .toString();
        return query;
    }

    public static String getStopQuery() {
        String query = new StringBuilder()
                .append("SELECT ")
                .append("   [Gid], [Number] ")
                .append("FROM [ptDOI4_Community].[dbo].[JourneyPatternPoint] AS JPP ")
                .append("GROUP BY JPP.Gid, JPP.Number ")
                .toString();
        return query;
    }

    public static String getMetroJourneyQuery(final int queryHistoryInDays, final int queryFutureInDays) {
        final String from = formatDate(-queryHistoryInDays);
        final String to = formatDate(queryFutureInDays);
        String query = new StringBuilder()
                .append("SELECT ")
                .append("   DISTINCT CONVERT(CHAR(16), DVJ.Id) AS " + DVJ_ID + ", ")
                .append("   KVV.StringValue AS " + ROUTE_NAME + ", ")
                .append("   SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1) AS " + DIRECTION + ", ")
                .append("   CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS " + OPERATING_DAY + ", ")
                .append("   RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2) ")
                .append("       + ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime)) ")
                .append("       - ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS " + START_TIME + ", ")
                .append("   CONVERT(CHAR(7), JPP.Number) AS " + STOP_NUMBER + " ")
                .append("FROM ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ ")
                .append("LEFT JOIN ptDOI4_Community.dbo.VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id) ")
                .append("LEFT JOIN ptDOI4_Community.T.KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number) ")
                .append("LEFT JOIN ptDOI4_Community.dbo.JourneyPatternPoint AS JPP ON (VJT.StartsAtJourneyPatternPointGid = JPP.Gid) ")
                .append("WHERE ")
                .append("   ( ")
                .append("       KT.Name = 'JoreIdentity' ")
                .append("       OR KT.Name = 'JoreRouteIdentity' ")
                .append("       OR KT.Name = 'RouteName' ")
                .append("   ) ")
                .append("   AND OT.Name = 'VehicleJourney' ")
                .append("   AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL ")
                .append("   AND DVJ.OperatingDayDate >= '" + from + "' ")
                .append("   AND DVJ.OperatingDayDate < '" + to + "' ")
                .append("   AND DVJ.IsReplacedById IS NULL ")
                .append("   AND VJT.TransportModeCode = 'METRO' ")
                .toString();
        return query;
    }

    public static String formatDate(int offsetInDays) {
        LocalDate now = LocalDate.now();
        LocalDate then = now.plus(offsetInDays, ChronoUnit.DAYS);

        String formattedString = DateTimeFormatter.ISO_LOCAL_DATE.format(then);
        log.debug("offsetInDays results to date " + formattedString);

        return formattedString;
    }
}
