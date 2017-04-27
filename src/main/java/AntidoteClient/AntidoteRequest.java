package main.java.AntidoteClient;

import com.google.protobuf.AbstractMessage;

/**
 * The Class AntidoteRequest.
 */
public class AntidoteRequest {

    /**
     * The code.
     */
    private final int code;

    /**
     * The message.
     */
    private final AbstractMessage message;

    /**
     * Instantiates a new antidote request.
     *
     * @param c the code
     * @param m the message
     */
    public AntidoteRequest(int c, AbstractMessage m) {
        code = c;
        message = m;
    }

    /**
     * Gets the length.
     *
     * @return the length
     */
    public int getLength() {
        return message.getSerializedSize() + 1;
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
    public AbstractMessage getMessage() {
        return message;
    }
}
