package eu.dauphine;

public class AlertIp {
    public String ip;

    public Alert() {

    }
    public Alert(String ip) {
        this.ip = ip;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "ip='" + ip + '\'' +
                '}';
    }
}
