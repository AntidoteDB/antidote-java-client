import com.basho.riak.protobuf.AntidotePB.*;
public class AntidoteClientExample {
    public static void main(String[] args) {
        AntidoteClient antidoteClient = new AntidoteClient("localhost", 8087);
        antidoteClient.updateCounter("testCounter", "testBucket");
        antidoteClient.readCounter("testCounter", "testBucket");
    }
}
