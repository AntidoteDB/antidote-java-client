package main.java.AntidoteClient;

public class Host {
    private String hostname;
    private int port;

    public Host(String h, int p) {
        hostname = h;
        port = p;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }
}
