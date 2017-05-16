package eu.antidotedb.client;

import eu.antidotedb.client.transformer.Transformer;
import eu.antidotedb.client.transformer.TransformerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * The Class Connection.
 */
public class Connection implements AutoCloseable {

    /**
     * The pool.
     */
    private final ConnectionPool pool;

    /**
     * The socket.
     */
    private final Socket socket;

    /**
     *
     */
    private Transformer transformer;

    /**
     * Instantiates a new connection.
     *
     * @param p the p
     * @param s the s
     */
    public Connection(ConnectionPool p, Socket s, List<TransformerFactory> transformerFactories) {
        pool = p;
        socket = s;
        transformer = new SocketSender(s);
        for (TransformerFactory transformerFactory : transformerFactories) {
            transformer = transformerFactory.newTransformer(transformer, this);
        }
    }

    /**
     * Return connection.
     */
    public void returnConnection() {
        pool.surrenderConnection(this);
    }

    public void setunHealthyConnection() {
        pool.setHealthy(false);
    }

    @Override
    public void close() {
        returnConnection();
    }

    public Transformer transformer() {
        return transformer;
    }

    void discard() {
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        }
    }

    Socket getSocket() {
        return socket;
    }
}
