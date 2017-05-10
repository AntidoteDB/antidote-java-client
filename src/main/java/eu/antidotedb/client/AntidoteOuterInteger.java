package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;

import java.util.OptionalLong;

/**
 * The Class AntidoteOuterInteger.
 */
public final class AntidoteOuterInteger extends AntidoteCRDT {

    /**
     * The value of the integer.
     */
    private long value;

    private long delta;

    private OptionalLong assigned = OptionalLong.empty();


    /**
     * The low level integer.
     */
    private final IntegerRef ref;

    /**
     * Instantiates a new antidote integer.
     */
    AntidoteOuterInteger(IntegerRef ref) {
        this.ref = ref;
    }

    /**
     * Gets the value.
     */
    public long getValue() {
        return value;
    }

    /**
     * Sets the value.
     */
    public void set(int newValue) {
        value = newValue;
        assigned = OptionalLong.of(newValue);
        delta = 0;
    }


    /**
     * Increment by one.
     */
    public void increment() {
        increment(1);
    }

    /**
     * Increment by inc.
     *
     * @param inc the value by which the integer is incremented
     */
    public void increment(int inc) {
        value += inc;
        delta += inc;
    }

    @Override
    public IntegerRef getRef() {
        return ref;
    }

    @Override
    void updateFromReadResponse(AntidotePB.ApbReadObjectResp object) {
        value = object.getInt().getValue();
        assigned = OptionalLong.empty();
        delta = 0;

    }

    @Override
    public void push(AntidoteTransaction tx) {
        if (assigned.isPresent()) {
            ref.set(tx, assigned.getAsLong());
        }
        if (delta != 0) {
            ref.increment(delta, tx);
        }
        assigned = OptionalLong.empty();
        delta = 0;
    }
}
