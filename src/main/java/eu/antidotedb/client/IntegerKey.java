package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;

public class IntegerKey extends Key<Long> {
    IntegerKey(ByteString key) {
        super(AntidotePB.CRDT_type.INTEGER, key);
    }


    /**
     * Creates an update operation to increment the integer.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOp increment(long inc) {
        AntidotePB.ApbIntegerUpdate.Builder op = AntidotePB.ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        op.setInc(inc); // Set increment
        AntidotePB.ApbUpdateOperation.Builder updateOperation = AntidotePB.ApbUpdateOperation.newBuilder();
        updateOperation.setIntegerop(op);
        return new UpdateOpDefaultImpl(this, updateOperation);
    }

    /**
     * Creates an update operation to assign a new value to the integer.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOp assign(long inc) {
        AntidotePB.ApbIntegerUpdate.Builder op = AntidotePB.ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        op.setSet(inc); // Set increment
        AntidotePB.ApbUpdateOperation.Builder updateOperation = AntidotePB.ApbUpdateOperation.newBuilder();
        updateOperation.setIntegerop(op);
        return new UpdateOpDefaultImpl(this, updateOperation);
    }

    @Override
    Long readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.integer().readResponseToValue(resp);
    }
}
