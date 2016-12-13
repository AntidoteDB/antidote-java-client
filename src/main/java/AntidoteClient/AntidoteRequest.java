package main.java.AntidoteClient;
import com.google.protobuf.AbstractMessage;

public class AntidoteRequest {
    private int code;
    private AbstractMessage message;

    public AntidoteRequest(int c, AbstractMessage m) {
        code = c;
        message = m;
    }

    public int getLength() {
        return message.getSerializedSize() + 1;
    }

    public int getCode() {
        return code;
    }

    public AbstractMessage getMessage() {
        return message;
    }
}
