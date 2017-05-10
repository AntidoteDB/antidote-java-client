package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.basho.riak.protobuf.AntidotePB.ApbIntegerUpdate;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelInteger.
 */
public final class IntegerRef extends ObjectRef {


    public IntegerRef(CrdtContainer container, ByteString key) {
        super(container, key, AntidotePB.CRDT_type.INTEGER);
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
    public void increment(long inc, AntidoteTransaction tx) {
        getContainer().update(tx, getType(), getKey(), incrementOpBuilder(inc));
    }

    /**
     * Sets the counter to the given value
     */
    public void set(AntidoteTransaction tx, long value) {
        getContainer().update(tx, getType(), getKey(), setOpBuilder(value));
    }


    @Override
    public Long read(TransactionWithReads tx) {
        return getContainer().read(tx, getType(), getKey()).getInt().getValue();
    }


    /**
     * Prepare the increment operation builder.
     *
     * @param value the value
     * @return the apb update operation. builder
     */
    protected ApbUpdateOperation.Builder incrementOpBuilder(long value) {
        ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setInc(value); //Set the integer to this value
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setIntegerop(intUpdateInstruction);
        return updateOperation;
    }

    /**
     * Prepare the set operation builder.
     *
     * @param value the value
     * @return the apb update operation. builder
     */
    protected ApbUpdateOperation.Builder setOpBuilder(long value) {
        ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setSet(value); //Set the integer to this value
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setIntegerop(intUpdateInstruction);
        return updateOperation;
    }

}
