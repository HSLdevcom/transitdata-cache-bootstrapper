package fi.hsl.transitdata.pubtransredisconnect.model;

import fi.hsl.common.transitdata.JoreDateTime;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class MetroJourneyResult {

    private final String dvjId;
    private final String operatingDay;
    private final String startTime;
    private final String stopNumber;
    private final String routeName;
    private final String direction;
    private final String dateTime;

    public MetroJourneyResult(String dvjId, String operatingDay, String startTime, String stopNumber, String routeName, String direction) throws ParseException {
        this.dvjId = dvjId;
        this.operatingDay = operatingDay;
        this.startTime = startTime;
        this.stopNumber = stopNumber;
        this.routeName = routeName;
        this.direction = direction;
        this.dateTime = processDateTime(operatingDay, startTime);
    }

    public String getDvjId() {
        return dvjId;
    }

    public String getOperatingDay() {
        return operatingDay;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getStopNumber() {
        return stopNumber;
    }

    public String getRouteName() {
        return routeName;
    }

    public String getDirection() {
        return direction;
    }

    public String getDateTime() {
        return dateTime;
    }

    private static String processDateTime(final String operatingDay, final String startTime) throws ParseException {
        LocalDate date = LocalDate.parse(operatingDay, DateTimeFormatter.BASIC_ISO_DATE);
        ZonedDateTime dateTime = ZonedDateTime.of(date, LocalTime.MIN, ZoneOffset.UTC);
        dateTime = dateTime.plusSeconds(JoreDateTime.timeStringToSeconds(startTime));

        return DateTimeFormatter.ISO_INSTANT.format(dateTime);
    }
}
