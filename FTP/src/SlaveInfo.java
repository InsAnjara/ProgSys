public class SlaveInfo {
    private String ip;
    private int port;

    public SlaveInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "SlaveInfo{ip='" + ip + "', port=" + port + "}";
    }
}
