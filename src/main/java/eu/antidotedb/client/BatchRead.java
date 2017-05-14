package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import eu.antidotedb.client.messages.AntidoteRequest;
import eu.antidotedb.client.messages.AntidoteRequest.MsgStaticReadObjects;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class BatchRead {

    private ArrayList<BatchReadResultImpl> requests = new ArrayList<>();
    private AntidoteClient antidoteClient;


    BatchRead(AntidoteClient antidoteClient) {
        this.antidoteClient = antidoteClient;
    }

    /**
     * Commits this batch-read
     */
    public void commit() {
        if (requests.isEmpty()) {
            // nothing to do
            return;
        }
        AntidotePB.ApbStaticReadObjects.Builder readObject = AntidotePB.ApbStaticReadObjects.newBuilder();
        for (BatchReadResultImpl request : requests) {
            readObject.addObjects(request.getObject());
        }
        readObject.setTransaction(AntidotePB.ApbStartTransaction.newBuilder().build());

        AntidotePB.ApbStaticReadObjects readObjectsMessage = readObject.build();
        Connection connection = antidoteClient.getPoolManager().getConnection();
        MsgStaticReadObjects request = AntidoteRequest.of(readObjectsMessage);
        AntidotePB.ApbStaticReadObjectsResp readResponse = antidoteClient.sendMessage(request, connection);
        int i = 0;
        for (AntidotePB.ApbReadObjectResp resp : readResponse.getObjects().getObjectsList()) {
            requests.get(i).setResult(resp);
            i++;
        }
        requests.clear();
    }

    public BatchReadResultImpl readHelper(ByteString bucket, ByteString key, AntidotePB.CRDT_type type) {
        AntidotePB.ApbBoundObject.Builder object = AntidotePB.ApbBoundObject.newBuilder(); // The object in the message to update
        object.setKey(key);
        object.setType(type);
        object.setBucket(bucket);

        BatchReadResultImpl res = new BatchReadResultImpl(this, object);
        requests.add(res);
        return res;
    }
}
