package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

public abstract class InnerUpdateOp {
    private Key<?> key;

    public InnerUpdateOp(Key<?> key) {
        this.key = key;
    }

    public Key<?> getKey() {
        return key;
    }

    abstract AntidotePB.ApbUpdateOperation.Builder getApbUpdate();

    AntidotePB.ApbMapNestedUpdate.Builder toApbNestedUpdate() {
        return AntidotePB.ApbMapNestedUpdate.newBuilder()
                .setKey(key.toApbMapKey())
                .setUpdate(getApbUpdate());
    }

    AntidotePB.ApbUpdateOp.Builder getApbUpdate(ByteString bucket) {
        AntidotePB.ApbBoundObject.Builder boundObject = AntidotePB.ApbBoundObject.newBuilder()
                .setBucket(bucket)
                .setType(key.getType())
                .setKey(key.getKey());
        return AntidotePB.ApbUpdateOp.newBuilder()
                .setBoundobject(boundObject)
                .setOperation(getApbUpdate());
    }
}
