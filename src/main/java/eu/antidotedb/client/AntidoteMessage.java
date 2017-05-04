package eu.antidotedb.client;

/**
 * The Class AntidoteMessage.
 */
public final class AntidoteMessage {
    
    /** The length. */
    private final int length;
    
    /** The code. */
    private final int code;
    
    /** The message. */
    private final byte[] message;

    /**
     * Instantiates a new antidote message.
     *
     * @param l the length
     * @param c the code
     * @param m the message
     */
    public AntidoteMessage(int l, int c, byte[] m) {
        length = l;
        code = c;
        message = m;
    }

    /**
     * Gets the length.
     *
     * @return the length
     */
    public int getLength() {
        return length;
    }

    /**
     * Gets the code.
     *
     * @return the code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets the message.
     *
     * @return the message
     */
    public byte[] getMessage() {
        return message;
    }
}
