package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

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
        AntidotePB.ApbStaticUpdateObjects.Builder updateMessage = AntidotePB.ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote
        AntidotePB.ApbStartTransaction.Builder startTransaction = AntidotePB.ApbStartTransaction.newBuilder();
        updateMessage.setTransaction(startTransaction);
        updateMessage.addUpdates(updateInstruction);
        AntidoteMessage responseMessage =
                client.sendMessage(new AntidoteRequest(
                        RiakPbMsgs.ApbStaticUpdateObjects,
                        updateMessage.build()));
        try {
            AntidotePB.ApbCommitResp commitResponse = AntidotePB.ApbCommitResp.parseFrom(responseMessage.getMessage());
            client.completeTransaction(commitResponse);
        } catch (InvalidProtocolBufferException e) {
            throw new AntidoteException("Could not parse commit response", e);
        }
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

        AntidoteMessage responseMessage =
                client.sendMessage(new AntidoteRequest(
                        RiakPbMsgs.ApbStaticReadObjects,
                        readMessage.build()));
        try {
            AntidotePB.ApbStaticReadObjectsResp resp = AntidotePB.ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            return resp.getObjects();
        } catch (InvalidProtocolBufferException e) {
            throw new AntidoteException("Could not parse read objects response", e);
        }
    }

}
