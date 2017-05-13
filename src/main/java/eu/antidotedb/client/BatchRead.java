package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class BatchRead {

    private ArrayList<BatchReadResultImpl> requests = new ArrayList<>();
    private AntidoteClient antidoteClient;


    public BatchRead(AntidoteClient antidoteClient) {
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
        AntidoteRequest request = new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readObjectsMessage);
        AntidoteMessage readMessage = antidoteClient.sendMessage(request, connection);
        AntidotePB.ApbStaticReadObjectsResp readResponse;
        try {
            readResponse = AntidotePB.ApbStaticReadObjectsResp.parseFrom(readMessage.getMessage());
        } catch (InvalidProtocolBufferException e) {
            throw new AntidoteException("Could not parse read response for objects", e);
        }
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
