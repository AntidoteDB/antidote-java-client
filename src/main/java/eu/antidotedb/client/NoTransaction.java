package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.client.messages.AntidoteRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class can be used to execute an individual operation without any transactional context
 */
public class NoTransaction extends TransactionWithReads {

    private final AntidoteClient client;
    private final CommitInfo timestamp;
    private CommitInfo lastCommitTimestamp;

    public NoTransaction(AntidoteClient client) {
        this(client, null);
    }

    public NoTransaction(AntidoteClient client, CommitInfo timestamp) {
        this.client = client;
        this.timestamp = timestamp;
    }

    @Override
    void performUpdate(AntidotePB.ApbUpdateOp.Builder updateInstruction) {
        performUpdates(Collections.singletonList(updateInstruction));
    }

    @Override
    void performUpdates(Collection<AntidotePB.ApbUpdateOp.Builder> updateInstructions) {
        AntidotePB.ApbStaticUpdateObjects.Builder updateMessage = AntidotePB.ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote
        AntidotePB.ApbStartTransaction.Builder startTransaction = AntidoteStaticTransaction.newStartTransaction(timestamp);
        updateMessage.setTransaction(startTransaction);
        for (AntidotePB.ApbUpdateOp.Builder updateInstruction : updateInstructions) {
            updateMessage.addUpdates(updateInstruction);
        }
        AntidotePB.ApbCommitResp commitResponse =
                client.sendMessageArbitraryConnection(this, AntidoteRequest.of(updateMessage.build()));
        lastCommitTimestamp = client.completeTransaction(commitResponse);
    }

    @Override
    AntidotePB.ApbReadObjectsResp readHelper(ByteString bucket, ByteString key, AntidotePB.CRDT_type type) {
        AntidotePB.ApbStaticReadObjects.Builder readMessage = AntidotePB.ApbStaticReadObjects.newBuilder();
        AntidotePB.ApbBoundObject.Builder obj = AntidotePB.ApbBoundObject.newBuilder()
                .setBucket(bucket)
                .setType(type)
                .setKey(key);
        readMessage.addObjects(obj);
        AntidotePB.ApbStartTransaction.Builder startTransaction = AntidoteStaticTransaction.newStartTransaction(timestamp);
        readMessage.setTransaction(startTransaction);

        AntidotePB.ApbStaticReadObjectsResp resp =
                client.sendMessageArbitraryConnection(this, AntidoteRequest.of(readMessage.build()));
        client.completeTransaction(resp.getCommittime());
        return resp.getObjects();
    }

    @Override
    void batchReadHelper(List<BatchReadResultImpl> requests) {
        AntidotePB.ApbStaticReadObjects.Builder readObject = AntidotePB.ApbStaticReadObjects.newBuilder();
        for (BatchReadResultImpl request : requests) {
            readObject.addObjects(request.getObject());
        }
        readObject.setTransaction(AntidoteStaticTransaction.newStartTransaction(timestamp));

        AntidotePB.ApbStaticReadObjects readObjectsMessage = readObject.build();
        Connection connection = client.getPoolManager().getConnection();
        try {
            onGetConnection(connection);
            AntidoteRequest.MsgStaticReadObjects request = AntidoteRequest.of(readObjectsMessage);
            AntidotePB.ApbStaticReadObjectsResp readResponse = client.sendMessage(request, connection);
            int i = 0;
            for (AntidotePB.ApbReadObjectResp resp : readResponse.getObjects().getObjectsList()) {
                requests.get(i).setResult(resp);
                i++;
            }
        } finally {
            onReleaseConnection(connection);
            connection.close();
        }
    }

    public CommitInfo getLastCommitTimestamp() {
        return lastCommitTimestamp;
    }
}
