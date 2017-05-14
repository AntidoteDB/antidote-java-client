package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbReadObjectResp;
import com.google.protobuf.ByteString;

public class Bucket implements CrdtContainer {
    private final ByteString name;

    public Bucket(ByteString name) {
        this.name = name;
    }

    public ByteString getName() {
        return name;
    }


    @Override
    public ApbReadObjectResp read(TransactionWithReads tx, AntidotePB.CRDT_type type, ByteString key) {
        AntidotePB.ApbReadObjectsResp apbReadObjectsResp = tx.readHelper(name, key, type);
        return apbReadObjectsResp.getObjects(0);
    }

    @Override
    public BatchReadResult<ApbReadObjectResp> readBatch(BatchRead tx, AntidotePB.CRDT_type type, ByteString key) {
        return tx.readHelper(name, key, type);
    }

    @Override
    public void update(AntidoteTransaction tx, AntidotePB.CRDT_type type, ByteString key, AntidotePB.ApbUpdateOperation.Builder builder) {
        AntidotePB.ApbUpdateOp.Builder update = AntidotePB.ApbUpdateOp.newBuilder();
        AntidotePB.ApbBoundObject.Builder boundObject = AntidotePB.ApbBoundObject.newBuilder();
        boundObject.setBucket(name);
        boundObject.setKey(key);
        boundObject.setType(type);
        update.setBoundobject(boundObject);
        update.setOperation(builder);
        tx.performUpdate(update);
    }
}
