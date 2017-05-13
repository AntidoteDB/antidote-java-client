package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.google.protobuf.ByteString;

/**
 * The Class CrdtCounter.
 */
public final class CrdtCounter extends AntidoteCRDT {

    private static final CrdtCreator<CrdtCounter> CREATOR = new CrdtCreator<CrdtCounter>() {
        @Override
        public AntidotePB.CRDT_type type() {
            return AntidotePB.CRDT_type.COUNTER;
        }

        @Override
        public CrdtCounter create(CrdtContainer c, ByteString key) {
            return c.counter(key).createAntidoteCounter();
        }

        @Override
        public CrdtCounter cast(AntidoteCRDT value) {
            return (CrdtCounter) value;
        }
    };


    /**
     * The value of the counter.
     */
    private int value;

    private int delta;

    private final CounterRef ref;


    public CrdtCounter(CounterRef ref) {
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
        ref.increment(tx, delta);
        delta = 0;
    }

    public static CrdtCreator<CrdtCounter> creator() {
        return CREATOR;
    }
}
