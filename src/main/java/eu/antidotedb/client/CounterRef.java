package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.ApbCounterUpdate;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelCounter.
 */
public final class CounterRef extends ObjectRef {


    public CounterRef(CrdtContainer container, ByteString key, CRDT_type type) {
        super(container, key, type);
    }

    /**
     * Increments the counter by one
     */
    public void increment(AntidoteTransaction tx) {
        increment(1, tx);
    }

    /**
     * Increments the counter by "inc".
     * <p>
     * Use negative values to decrement the counter.
     */
    public void increment(int inc, AntidoteTransaction tx) {
        getContainer().update(tx, getType(), getKey(), incrementOpBuilder(inc));
    }

    @Override
    public Integer read(TransactionWithReads tx) {
        return getContainer().read(tx, getType(), getKey()).getCounter().getValue();
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


    public AntidoteOuterCounter createAntidoteCounter() {
        return new AntidoteOuterCounter(this);
    }
}
