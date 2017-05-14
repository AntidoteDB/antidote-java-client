package eu.antidotedb.client;

/**
 * The Class AntidoteException.
 */
public class AntidoteException extends RuntimeException {

    /**
     * The Constant serialVersionUID.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new antidote exception.
     *
     * @param message the message
     */
    public AntidoteException(String message) {
        super(message);
    }

    public AntidoteException(String message, Throwable cause) {
        super(message, cause);
    }

    public AntidoteException(Throwable cause) {
        super(cause);
    }
}
