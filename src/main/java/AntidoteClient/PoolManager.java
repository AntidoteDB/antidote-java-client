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
        for(Host h : hosts) {
            pools.add(new ConnectionPool(maxPoolSize, initialPoolSize, h.getHostname(), h.getPort()));
        }
    }

    public Connection getConnection() {
        for(ConnectionPool pool : pools) {
            if (pool.isHealthy()) {
                try {
                    Socket s = pool.getConnection();
                    if (s != null) {
                        return new Connection(pool, s);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (++retries < 3) {
            return getConnection();
        } else {
            throw new RuntimeException("Cannot open connection");
        }
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
}
