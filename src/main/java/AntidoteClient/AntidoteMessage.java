package main.java.AntidoteClient;

/**
 * The Class AntidoteMessage.
 */
public class AntidoteMessage {
    
    /** The length. */
    private int length;
    
    /** The code. */
    private int code;
    
    /** The message. */
    private byte[] message;

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
