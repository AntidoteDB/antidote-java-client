package main.java.AntidoteClient;

import java.net.Socket;

/**
 * The Class Connection.
 */
public class Connection {
    
    /** The pool. */
    private ConnectionPool pool;
    
    /** The socket. */
    private Socket socket;

    /**
     * Instantiates a new connection.
     *
     * @param p the p
     * @param s the s
     */
    public Connection(ConnectionPool p, Socket s) {
        socket = s;
        pool =  p;
    }

    /**
     * Return connection.
     */
    public void returnConnection() {
        pool.surrenderConnection(socket);
    }

    /**
     * Gets the socket.
     *
     * @return the socket
     */
    public Socket getSocket() {
        return socket;
    }
}
