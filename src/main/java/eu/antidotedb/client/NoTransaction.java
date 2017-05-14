package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import com.google.protobuf.ByteString;
import eu.antidotedb.client.messages.AntidoteRequest;

import java.util.Collections;
import java.util.List;

/**
 * This class can be used to execute an individual operation without any transactional context
 */
public class NoTransaction extends TransactionWithReads {

    private final AntidoteClient client;

    public NoTransaction(AntidoteClient client) {
        this.client = client;
    }

    @Override
    void performUpdate(AntidotePB.ApbUpdateOp.Builder updateInstruction) {
        performUpdates(Collections.singletonList(updateInstruction));
    }

    @Override
    void performUpdates(List<AntidotePB.ApbUpdateOp.Builder> updateInstructions) {
        AntidotePB.ApbStaticUpdateObjects.Builder updateMessage = AntidotePB.ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote
        AntidotePB.ApbStartTransaction.Builder startTransaction = AntidotePB.ApbStartTransaction.newBuilder();
        updateMessage.setTransaction(startTransaction);
        for (AntidotePB.ApbUpdateOp.Builder updateInstruction : updateInstructions) {
            updateMessage.addUpdates(updateInstruction);
        }
        AntidotePB.ApbCommitResp commitResponse =
                client.sendMessageArbitraryConnection(AntidoteRequest.of(updateMessage.build()));
        client.completeTransaction(commitResponse);
    }

    @Override
    AntidotePB.ApbReadObjectsResp readHelper(ByteString bucket, ByteString key, AntidotePB.CRDT_type type) {
        AntidotePB.ApbStaticReadObjects.Builder readMessage = AntidotePB.ApbStaticReadObjects.newBuilder();
        AntidotePB.ApbBoundObject.Builder obj = AntidotePB.ApbBoundObject.newBuilder()
                .setBucket(bucket)
                .setType(type)
                .setKey(key);
        readMessage.addObjects(obj);
        AntidotePB.ApbStartTransaction.Builder startTransaction = AntidotePB.ApbStartTransaction.newBuilder();
        readMessage.setTransaction(startTransaction);

        AntidotePB.ApbStaticReadObjectsResp resp =
                client.sendMessageArbitraryConnection(AntidoteRequest.of(readMessage.build()));
        client.completeTransaction(resp.getCommittime());
        return resp.getObjects();
    }

    @Override
    void batchReadHelper(List<BatchReadResultImpl> requests) {
        AntidotePB.ApbStaticReadObjects.Builder readObject = AntidotePB.ApbStaticReadObjects.newBuilder();
        for (BatchReadResultImpl request : requests) {
            readObject.addObjects(request.getObject());
        }
        readObject.setTransaction(AntidotePB.ApbStartTransaction.newBuilder().build());

        AntidotePB.ApbStaticReadObjects readObjectsMessage = readObject.build();
        Connection connection = client.getPoolManager().getConnection();
        AntidoteRequest.MsgStaticReadObjects request = AntidoteRequest.of(readObjectsMessage);
        AntidotePB.ApbStaticReadObjectsResp readResponse = client.sendMessage(request, connection);
        int i = 0;
        for (AntidotePB.ApbReadObjectResp resp : readResponse.getObjects().getObjectsList()) {
            requests.get(i).setResult(resp);
            i++;
        }
    }

}
