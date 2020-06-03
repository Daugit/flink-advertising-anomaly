package eu.dauphine;

public class AlertUid {
    public String uid;

    public Alert() {

    }
    public Alert(String uid) {
        this.uid = uid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "uid='" + uid + '\'' +
                '}';
    }
}
