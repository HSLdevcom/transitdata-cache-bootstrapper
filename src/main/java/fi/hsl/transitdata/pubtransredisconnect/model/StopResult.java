package fi.hsl.transitdata.pubtransredisconnect.model;

public class StopResult implements DatabaseQueryResult {

    private final String gid;
    private final String number;

    public StopResult(String gid, String number) {
        this.gid = gid;
        this.number = number;
    }

    public String getGid() {
        return gid;
    }

    public String getNumber() {
        return number;
    }
}
