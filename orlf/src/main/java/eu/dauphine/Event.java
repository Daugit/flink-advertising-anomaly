package eu.dauphine;


import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Event {
    public String eventType;
    public String uid;
    public Long timestamp;
    public String ip;
    public String impressionId;

    public Event(String eventType, String uid, Long timestamp, String ip, String impressionId) {
        this.eventType = eventType;
        this.uid = uid;
        this.timestamp = timestamp;
        this.ip = ip;
        this.impressionId = impressionId;
    }

    public Event(String eventStr) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject json = (JSONObject) parser.parse(eventStr);

        this.eventType = (String) json.get("eventType");
        this.uid = (String) json.get("uid");
        this.timestamp = (Long) json.get("timestamp");
        this.ip = (String) json.get("ip");
        this.impressionId = (String) json.get("impressionId");
    }

    @Override
    public String toString() {
        return "Event{" +
                "eventType='" + eventType + '\'' +
                ", uid='" + uid + '\'' +
                ", timestamp=" + timestamp +
                ", ip='" + ip + '\'' +
                ", impressionId='" + impressionId + '\'' +
                '}';
    }
}