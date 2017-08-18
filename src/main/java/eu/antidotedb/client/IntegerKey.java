package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;

public class IntegerKey extends Key {
    public IntegerKey(ByteString key) {
        super(AntidotePB.CRDT_type.INTEGER, key);
    }

    @CheckReturnValue
    public InnerUpdateOp increment(long inc) {
        AntidotePB.ApbIntegerUpdate.Builder op = AntidotePB.ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        op.setInc(inc); // Set increment
        AntidotePB.ApbUpdateOperation.Builder updateOperation = AntidotePB.ApbUpdateOperation.newBuilder();
        updateOperation.setIntegerop(op);
        return new InnerUpdateOpImpl(this, updateOperation);
    }

    @CheckReturnValue
    public InnerUpdateOp assign(long inc) {
        AntidotePB.ApbIntegerUpdate.Builder op = AntidotePB.ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        op.setSet(inc); // Set increment
        AntidotePB.ApbUpdateOperation.Builder updateOperation = AntidotePB.ApbUpdateOperation.newBuilder();
        updateOperation.setIntegerop(op);
        return new InnerUpdateOpImpl(this, updateOperation);
    }

    @Override
    Object readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.integer().readResponseToValue(resp);
    }
}
