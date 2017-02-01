package main.java.AntidoteClient;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

public class PoolManager {
    private List<ConnectionPool> pools = new LinkedList<ConnectionPool>();
    private int retries = 0;

    public PoolManager(int maxPoolSize, int initialPoolSize, List<Host> hosts) {
        for (Host h : hosts) {
            pools.add(new ConnectionPool(maxPoolSize, initialPoolSize, h.getHostname(), h.getPort()));
        }
    }

    public Connection getConnection() {
        int selectedIndex = 0;
        List<ConnectionPool> healthyPools = getHealthyPools();
        if (healthyPools.size() > 0) {
            do {
                for (int i = 0; i < healthyPools.size(); i++) {
                    selectedIndex = (int) (Math.random() * healthyPools.size());

                    ConnectionPool p = healthyPools.get(selectedIndex);
                    if (p.isHealthy()) {
                        try {
                            Socket s = p.getConnection();
                            if (s != null) {
                                return new Connection(p, s);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            } while (++retries < 3);
        }
        throw new RuntimeException("Cannot open connection");
    }

    public AntidoteMessage sendMessage(AntidoteRequest requestMessage) {
        Connection c = this.getConnection();
        try {
            DataOutputStream dataOutputStream = new DataOutputStream(c.getSocket().getOutputStream());
            dataOutputStream.writeInt(requestMessage.getLength());
            dataOutputStream.writeByte(requestMessage.getCode());
            requestMessage.getMessage().writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(c.getSocket().getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            return new AntidoteMessage(responseLength, responseCode, messageData);

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        } finally {
            c.returnConnection();
        }
    }

    private List<ConnectionPool> getHealthyPools() {
        List<ConnectionPool> healthyPools = new LinkedList<ConnectionPool>();
        for (ConnectionPool p: pools) {
            if(p.isHealthy()) {
                healthyPools.add(p);
            }
        }
        return healthyPools;
    }
}
