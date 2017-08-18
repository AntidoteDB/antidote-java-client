package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

/**
 * An update operation which can be executed with methods on {@link Bucket}.
 * To create an update operation use the methods defined on the various subclassed of {@link Key}.
 */
public abstract class UpdateOp {
    private Key<?> key;

    UpdateOp(Key<?> key) {
        this.key = key;
    }

    /**
     * The key of this update operation.
     */
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
