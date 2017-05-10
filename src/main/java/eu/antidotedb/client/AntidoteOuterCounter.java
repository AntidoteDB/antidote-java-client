package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;

/**
 * The Class AntidoteOuterCounter.
 */
public final class AntidoteOuterCounter extends AntidoteCRDT {

    /**
     * The value of the counter.
     */
    private int value;

    private int delta;

    private final CounterRef ref;


    public AntidoteOuterCounter(CounterRef ref) {
        this.ref = ref;
    }

    /**
     * Gets the current value.
     */
    public int getValue() {
        return value;
    }

    /**
     * Increment by one.
     */
    public void increment() {
        increment(1);
    }


    /**
     * Increment.
     *
     * @param inc the value by which the counter is incremented
     */
    public void increment(int inc) {
        value += inc;
        delta += inc;
    }

    @Override
    public ObjectRef getRef() {
        return ref;
    }

    @Override
    public void updateFromReadResponse(AntidotePB.ApbReadObjectResp readResponse) {
        value = readResponse.getCounter().getValue();
        delta = 0;
    }

    @Override
    public void push(AntidoteTransaction tx) {
        ref.increment(delta, tx);
        delta = 0;
    }
}
