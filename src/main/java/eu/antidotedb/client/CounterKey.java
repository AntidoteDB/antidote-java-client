package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;

public class CounterKey extends Key<Integer> {

    CounterKey(AntidotePB.CRDT_type type, ByteString key) {
        super(type, key);
    }

    @Override
    Integer readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.counter().readResponseToValue(resp);
    }

    @CheckReturnValue
    public InnerUpdateOp increment(long inc) {
        AntidotePB.ApbCounterUpdate.Builder counterUpdateInstruction = AntidotePB.ApbCounterUpdate.newBuilder(); // The specific instruction in update instructions
        counterUpdateInstruction.setInc(inc); // Set increment
        AntidotePB.ApbUpdateOperation.Builder updateOperation = AntidotePB.ApbUpdateOperation.newBuilder();
        updateOperation.setCounterop(counterUpdateInstruction);
        return new InnerUpdateOpImpl(this, updateOperation);
    }




}
