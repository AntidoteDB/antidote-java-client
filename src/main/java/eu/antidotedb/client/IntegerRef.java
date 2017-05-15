package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbIntegerUpdate;
import eu.antidotedb.antidotepb.AntidotePB.ApbUpdateOperation;

/**
 * The Class LowLevelInteger.
 */
public final class IntegerRef extends ObjectRef<Long> {


    public IntegerRef(CrdtContainer<?> container, ByteString key) {
        super(container, key, AntidotePB.CRDT_type.INTEGER);
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
    public void increment(AntidoteTransaction tx, long inc) {
        getContainer().update(tx, getType(), getKey(), incrementOpBuilder(inc));
    }

    /**
     * Sets the counter to the given value
     */
    public void set(AntidoteTransaction tx, long value) {
        getContainer().update(tx, getType(), getKey(), setOpBuilder(value));
    }


    @Override
    Long readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return resp.getInt().getValue();
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

    public CrdtInteger toMutable() {
        return new CrdtInteger(this);
    }
}
