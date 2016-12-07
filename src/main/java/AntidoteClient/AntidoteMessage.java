package AntidoteClient;
public class AntidoteMessage {
    private int length;
    private int code;
    private byte[] message;

    public AntidoteMessage(int l, int c, byte[] m) {
        length = l;
        code = c;
        message = m;
    }

    public int getLength() {
        return length;
    }

    public int getCode() {
        return code;
    }

    public byte[] getMessage() {
        return message;
    }
}
