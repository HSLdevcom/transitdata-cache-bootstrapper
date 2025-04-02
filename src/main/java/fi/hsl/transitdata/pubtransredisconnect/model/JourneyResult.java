package fi.hsl.transitdata.pubtransredisconnect.model;

public class JourneyResult {

    private final String dvjId;
    private final String routeName;
    private final String direction;
    private final String startTime;
    private final String operatingDay;

    public JourneyResult(String dvjId, String routeName, String direction, String startTime, String operatingDay) {
        this.dvjId = dvjId;
        this.routeName = routeName;
        this.direction = direction;
        this.startTime = startTime;
        this.operatingDay = operatingDay;
    }

    public String getDvjId() {
        return dvjId;
    }

    public String getRouteName() {
        return routeName;
    }

    public String getDirection() {
        return direction;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getOperatingDay() {
        return operatingDay;
    }
}
