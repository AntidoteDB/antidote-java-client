package main.java.AntidoteClient;

import java.net.Socket;

public class Connection {
    private ConnectionPool pool;
    private Socket socket;

    public Connection(ConnectionPool p, Socket s) {
        socket = s;
        pool =  p;
    }

    public void returnConnection() {
        pool.surrenderConnection(socket);
    }

    public Socket getSocket() {
        return socket;
    }
}
