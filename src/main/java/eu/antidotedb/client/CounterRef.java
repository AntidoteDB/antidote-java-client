package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbCounterUpdate;
import eu.antidotedb.antidotepb.AntidotePB.ApbUpdateOperation;
import eu.antidotedb.antidotepb.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelCounter.
 */
public final class CounterRef extends ObjectRef<Integer> {


    public CounterRef(CrdtContainer container, ByteString key, CRDT_type type) {
        super(container, key, type);
    }

    /**
     * Increments the counter by one
     */
    public void increment(AntidoteTransaction tx) {
        increment(tx, 1);
    }

    /**
     * Increments the counter by "inc".
     * <p>
     * Use negative values to decrement the counter.
     */
    public void increment(AntidoteTransaction tx, int inc) {
        getContainer().update(tx, getType(), getKey(), incrementOpBuilder(inc));
    }


    @Override
    Integer readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return resp.getCounter().getValue();
    }


    /**
     * Prepare the increment operation builder.
     *
     * @param inc the inc
     * @return the apb update operation. builder
     */
    protected ApbUpdateOperation.Builder incrementOpBuilder(int inc) {
        ApbCounterUpdate.Builder counterUpdateInstruction = ApbCounterUpdate.newBuilder(); // The specific instruction in update instructions
        counterUpdateInstruction.setInc(inc); // Set increment
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setCounterop(counterUpdateInstruction);
        return updateOperation;
    }


    public CrdtCounter createAntidoteCounter() {
        return new CrdtCounter(this);
    }
}
